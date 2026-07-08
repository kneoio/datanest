package com.semantyca.datanest.service;

import com.semantyca.core.dto.rls.RlsActionDTO;
import com.semantyca.core.model.user.IUser;
import com.semantyca.datanest.dto.req.BulkACLRequestDTO;
import com.semantyca.datanest.repository.AiAgentRepository;
import com.semantyca.datanest.repository.BrandRepository;
import com.semantyca.datanest.repository.EventRepository;
import com.semantyca.datanest.repository.ListenersRepository;
import com.semantyca.datanest.repository.ProfileRepository;
import com.semantyca.datanest.repository.SceneRepository;
import com.semantyca.datanest.repository.ScriptRepository;
import com.semantyca.datanest.repository.prompt.PromptRepository;
import com.semantyca.datanest.repository.soundfragment.SoundFragmentRepository;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

@ApplicationScoped
public class BulkACLService {
    private static final Logger LOGGER = LoggerFactory.getLogger(BulkACLService.class);

    public record SseEvent(String type, JsonObject data) {}

    private static class JobState {
        int total;
        int done;
        int failed;
        boolean finished;
    }

    private final Map<String, JobState> jobs = new ConcurrentHashMap<>();
    private final Map<String, List<Consumer<SseEvent>>> subscribers = new ConcurrentHashMap<>();

    private final SoundFragmentRepository soundFragmentRepository;
    private final BrandRepository brandRepository;
    private final EventRepository eventRepository;
    private final ScriptRepository scriptRepository;
    private final SceneRepository sceneRepository;
    private final AiAgentRepository aiAgentRepository;
    private final ProfileRepository profileRepository;
    private final ListenersRepository listenersRepository;
    private final PromptRepository promptRepository;

    @Inject
    public BulkACLService(
            SoundFragmentRepository soundFragmentRepository,
            BrandRepository brandRepository,
            EventRepository eventRepository,
            ScriptRepository scriptRepository,
            SceneRepository sceneRepository,
            AiAgentRepository aiAgentRepository,
            ProfileRepository profileRepository,
            ListenersRepository listenersRepository,
            PromptRepository promptRepository
    ) {
        this.soundFragmentRepository = soundFragmentRepository;
        this.brandRepository = brandRepository;
        this.eventRepository = eventRepository;
        this.scriptRepository = scriptRepository;
        this.sceneRepository = sceneRepository;
        this.aiAgentRepository = aiAgentRepository;
        this.profileRepository = profileRepository;
        this.listenersRepository = listenersRepository;
        this.promptRepository = promptRepository;
    }

    public void subscribe(String jobId, Consumer<SseEvent> consumer) {
        subscribers.computeIfAbsent(jobId, k -> new ArrayList<>()).add(consumer);
        JobState st = jobs.get(jobId);
        if (st != null) {
            consumer.accept(new SseEvent("snapshot", new JsonObject()
                    .put("total", st.total)
                    .put("done", st.done)
                    .put("failed", st.failed)
                    .put("finished", st.finished)));
        }
    }

    public void unsubscribe(String jobId, Consumer<SseEvent> consumer) {
        List<Consumer<SseEvent>> list = subscribers.get(jobId);
        if (list != null) {
            list.remove(consumer);
            if (list.isEmpty()) subscribers.remove(jobId);
        }
    }

    private void emit(String jobId, String type, JsonObject data) {
        List<Consumer<SseEvent>> list = subscribers.get(jobId);
        if (list != null) {
            SseEvent ev = new SseEvent(type, data);
            for (Consumer<SseEvent> c : new ArrayList<>(list)) {
                try { c.accept(ev); } catch (Exception ignore) { }
            }
        }
    }

    public void startJob(String jobId, BulkACLRequestDTO request, IUser user) {
        if (jobId == null || jobId.isBlank()) throw new IllegalArgumentException("jobId is required");

        String resourceType = request.getResourceType();
        if (resourceType == null || resourceType.isBlank()) {
            throw new IllegalArgumentException("resourceType is required");
        }

        List<UUID> documentIds = request.getDocumentIds();
        List<RlsActionDTO> actions = request.getActions();

        JobState st = new JobState();
        st.total = documentIds != null ? documentIds.size() : 0;
        st.done = 0;
        st.failed = 0;
        st.finished = false;
        jobs.put(jobId, st);
        LOGGER.info("Bulk ACL job started: jobId={}, resourceType={}, total={}, actions={}", jobId, resourceType, st.total, actions);

        emit(jobId, "started", new JsonObject().put("total", st.total));

        if (st.total == 0) {
            st.finished = true;
            emit(jobId, "done", new JsonObject().put("total", 0).put("done", 0).put("failed", 0));
            return;
        }

        processSequential(jobId, documentIds, 0, actions, resourceType, user)
                .subscribe().with(
                        ignored -> {},
                        err -> {
                            LOGGER.error("Bulk ACL job failed: {}", jobId, err);
                            st.finished = true;
                            emit(jobId, "error", new JsonObject().put("message", err.getMessage()));
                        }
                );
    }

    private Uni<Void> bulkUpdateACLForResource(String resourceType, UUID documentId, List<RlsActionDTO> actions, IUser user) {
        return switch (resourceType) {
            case "soundfragments" -> soundFragmentRepository.bulkUpdateACL(documentId, actions, user);
            case "brands" -> brandRepository.bulkUpdateACL(documentId, actions, user);
            case "events" -> eventRepository.bulkUpdateACL(documentId, actions, user);
            case "scripts" -> scriptRepository.bulkUpdateACL(documentId, actions, user);
            case "scenes" -> sceneRepository.bulkUpdateACL(documentId, actions, user);
            case "aiagents" -> aiAgentRepository.bulkUpdateACL(documentId, actions, user);
            case "profiles" -> profileRepository.bulkUpdateACL(documentId, actions, user);
            case "listeners" -> listenersRepository.bulkUpdateACL(documentId, actions, user);
            case "prompts" -> promptRepository.bulkUpdateACL(documentId, actions, user);
            default -> Uni.createFrom().failure(new IllegalArgumentException("Unknown resourceType: " + resourceType));
        };
    }

    private Uni<Void> processSequential(String jobId, List<UUID> documentIds, int idx,
                                        List<RlsActionDTO> actions, String resourceType, IUser user) {
        if (idx >= documentIds.size()) {
            JobState st = jobs.get(jobId);
            if (st != null) {
                st.finished = true;
                emit(jobId, "done", new JsonObject()
                        .put("total", st.total)
                        .put("done", st.done)
                        .put("failed", st.failed));
            }
            return Uni.createFrom().voidItem();
        }

        UUID documentId = documentIds.get(idx);

        LOGGER.info("Processing document {}/{}: {}", idx + 1, documentIds.size(), documentId);
        return bulkUpdateACLForResource(resourceType, documentId, actions, user)
                .onItem().invoke(v -> {
                    JobState st = jobs.get(jobId);
                    if (st != null) st.done += 1;
                    emit(jobId, "progress", new JsonObject()
                            .put("documentId", documentId.toString())
                            .put("success", true)
                            .put("done", st != null ? st.done : 0)
                            .put("total", st != null ? st.total : 0));
                })
                .onFailure().recoverWithItem(err -> {
                    JobState st = jobs.get(jobId);
                    if (st != null) st.failed += 1;
                    LOGGER.warn("Bulk ACL failed for document {}: {}", documentId, err.getMessage());
                    emit(jobId, "progress", new JsonObject()
                            .put("documentId", documentId.toString())
                            .put("success", false)
                            .put("message", err.getMessage()));
                    return null;
                })
                .chain(ignored -> processSequential(jobId, documentIds, idx + 1, actions, resourceType, user));
    }
}
