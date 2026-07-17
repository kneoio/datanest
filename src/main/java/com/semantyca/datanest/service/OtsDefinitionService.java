package com.semantyca.datanest.service;

import com.semantyca.core.dto.DocumentAccessDTO;
import com.semantyca.core.llm.AnthropicTextClient;
import com.semantyca.core.model.cnst.LanguageCode;
import com.semantyca.core.model.user.IUser;
import com.semantyca.core.model.user.SuperUser;
import com.semantyca.core.service.AbstractService;
import com.semantyca.core.service.UserService;
import com.semantyca.core.util.WebHelper;
import com.semantyca.datanest.config.DatanestConfig;
import com.semantyca.datanest.dto.OtsDefinitionDTO;
import com.semantyca.datanest.dto.script.RelativeSceneDTO;
import com.semantyca.datanest.messaging.CommandPublisher;
import com.semantyca.datanest.repository.OtsDefinitionRepository;
import com.semantyca.mixpla.model.Script;
import com.semantyca.mixpla.model.cnst.OtsRunStatus;
import com.semantyca.mixpla.dto.queue.command.CommandType;
import com.semantyca.mixpla.model.filter.OtsDefinitionFilter;
import com.semantyca.mixpla.model.stream.OtsDefinition;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

@ApplicationScoped
public class OtsDefinitionService extends AbstractService<OtsDefinition, OtsDefinitionDTO> {
    private static final Logger LOGGER = Logger.getLogger(OtsDefinitionService.class);
    private static final long NAME_MAX_TOKENS = 30;
    private static final String NAME_SYSTEM_PROMPT = """
            You generate short, friendly display names for a listener's personal radio stream, \
            based on a script template and the values the listener filled in. \
            Reply with the name only - no quotes, no explanation, 2 to 6 words, title case.""";

    private final OtsDefinitionRepository repository;
    private final ScriptService scriptService;
    private final AnthropicTextClient anthropicTextClient;
    private final DatanestConfig config;
    private final CommandPublisher commandPublisher;

    @Inject
    public OtsDefinitionService(UserService userService,
                                 OtsDefinitionRepository repository,
                                 ScriptService scriptService,
                                 AnthropicTextClient anthropicTextClient,
                                 DatanestConfig config,
                                 CommandPublisher commandPublisher) {
        super(userService);
        this.repository = repository;
        this.scriptService = scriptService;
        this.anthropicTextClient = anthropicTextClient;
        this.config = config;
        this.commandPublisher = commandPublisher;
    }

    public Uni<List<OtsDefinitionDTO>> getAllDTO(final int limit, final int offset, final IUser user, final OtsDefinitionFilter filter) {
        return repository.getAll(limit, offset, false, user, filter)
                .chain(list -> {
                    if (list.isEmpty()) {
                        return Uni.createFrom().item(List.of());
                    }
                    List<Uni<OtsDefinitionDTO>> unis = list.stream()
                            .map(this::mapToDTO)
                            .collect(Collectors.toList());
                    return Uni.join().all(unis).andFailFast();
                });
    }

    public Uni<Integer> getAllCount(final IUser user, final OtsDefinitionFilter filter) {
        return repository.getAllCount(user, false, filter);
    }

    @Override
    public Uni<OtsDefinitionDTO> getDTO(UUID id, IUser user, LanguageCode language) {
        return repository.findById(id, user, false).chain(this::mapToDTO);
    }

    public Uni<OtsDefinitionDTO> getNewDTO(UUID scriptId, IUser user) {
        return scriptService.getById(scriptId, SuperUser.build())
                .map(script -> {
                    OtsDefinitionDTO dto = new OtsDefinitionDTO();
                    dto.setScriptId(script.getId());
                    dto.setName(script.getName());
                    dto.setColor(script.getColor());
                    dto.setRequiredVariables(script.getRequiredVariables());
                    return dto;
                });
    }

    @Override
    public Uni<OtsDefinitionDTO> upsert(String id, OtsDefinitionDTO dto, IUser user, LanguageCode code) {
        if (dto.getBrandId() == null && dto.getAgentId() == null) {
            return Uni.createFrom().failure(
                    new IllegalArgumentException("agentId is required when brandId is not set"));
        }
        if ("new".equalsIgnoreCase(id) || id == null || id.isBlank()) {
            return create(dto, user);
        }
        return update(UUID.fromString(id), dto, user);
    }

    @Override
    public Uni<Integer> delete(String id, IUser user) {
        UUID otsId = UUID.fromString(id);
        return repository.findById(otsId, user, false)
                .chain(ots -> repository.archive(otsId, user)
                        .invoke(count -> {
                            if (count > 0 && ots.getSlugName() != null) {
                                commandPublisher.publishCommand(CommandType.JESOOS_STOP_OTS, "ots_deleted",
                                        Map.of("slug", ots.getSlugName()));
                            }
                        }));
    }

    private Uni<OtsDefinitionDTO> update(UUID id, OtsDefinitionDTO dto, IUser user) {
        return scriptService.getById(dto.getScriptId(), SuperUser.build())
                .chain(script -> calculateEstimatedDurationMin(dto.getScriptId(), user)
                        .chain(estimatedDurationMin -> {
                            OtsDefinition entity = buildEntity(dto);
                            entity.setName(script.getName());
                            entity.setEstimatedDurationMin(estimatedDurationMin);
                            entity.setChatContext(buildChatContext(script, dto.getUserVariables()));
                            return repository.update(id, entity, user);
                        }))
                .chain(this::mapToDTO);
    }

    private Uni<OtsDefinitionDTO> create(OtsDefinitionDTO dto, IUser user) {
        return scriptService.getById(dto.getScriptId(), SuperUser.build())
                .chain(script -> generateName(script, dto.getUserVariables())
                        .chain(name -> {
                            String slug = WebHelper.generateSlug(name + "-" + System.currentTimeMillis());
                            return repository.existsBySlug(slug)
                                    .chain(exists -> {
                                        if (exists) {
                                            return Uni.createFrom().failure(
                                                    new IllegalArgumentException("An ots definition with slug '" + slug + "' already exists"));
                                        }
                                        return calculateEstimatedDurationMin(dto.getScriptId(), user)
                                                .chain(estimatedDurationMin -> {
                                                    OtsDefinition entity = buildEntity(dto);
                                                    entity.setName(name);
                                                    entity.setSlugName(slug);
                                                    entity.setEstimatedDurationMin(estimatedDurationMin);
                                                    entity.setChatContext(buildChatContext(script, dto.getUserVariables()));
                                                    return repository.insert(entity, user);
                                                });
                                    });
                        })
                )
                .chain(this::mapToDTO);
    }

    private Uni<Integer> calculateEstimatedDurationMin(UUID scriptId, IUser user) {
        return scriptService.getScenesByScriptId(scriptId, user)
                .map(scenes -> {
                    int totalSeconds = scenes.stream()
                            .filter(scene -> scene instanceof RelativeSceneDTO)
                            .mapToInt(scene -> ((RelativeSceneDTO) scene).getDurationSeconds())
                            .sum();
                    return totalSeconds / 60;
                });
    }

    private OtsDefinition buildEntity(OtsDefinitionDTO dto) {
        OtsDefinition entity = new OtsDefinition();
        entity.setName(dto.getName());
        entity.setScriptId(dto.getScriptId());
        entity.setUserVariables(dto.getUserVariables());
        entity.setBrandId(dto.getBrandId());
        entity.setAgentId(dto.getAgentId());
        entity.setType(dto.getType());
        return entity;
    }

    private String buildChatContext(Script script, Map<String, Object> userVariables) {
        StringBuilder sb = new StringBuilder("Event: ").append(script.getName());
        if (userVariables != null) {
            for (Map.Entry<String, Object> entry : userVariables.entrySet()) {
                sb.append("\n").append(entry.getKey()).append(": ").append(entry.getValue());
            }
        }
        return sb.toString();
    }

    private Uni<String> generateName(Script script, Map<String, Object> userVariables) {
        // LLM name generation temporarily disabled - Anthropic model config out of date.
        return Uni.createFrom().item(script.getName());

        /*
        String variablesText = userVariables == null || userVariables.isEmpty()
                ? "(no variables)"
                : userVariables.entrySet().stream()
                        .map(e -> e.getKey() + ": " + e.getValue())
                        .collect(Collectors.joining("\n"));

        String userMessage = "Script name: " + script.getName() + "\nVariables:\n" + variablesText;

        return anthropicTextClient.createTextMessage(
                        config.getAnthropicApiKey(),
                        config.getAnthropicModel(),
                        NAME_MAX_TOKENS,
                        NAME_SYSTEM_PROMPT,
                        userMessage)
                .map(result -> result.text().trim())
                .onFailure().invoke(throwable -> LOGGER.errorf("Failed to generate ots definition name for script: %s", script.getId(), throwable));
        */
    }

    public Uni<List<DocumentAccessDTO>> getDocumentAccess(UUID documentId, IUser user) {
        return repository.getDocumentAccessInfo(documentId, user)
                .onItem().transform(accessInfoList ->
                        accessInfoList.stream()
                                .map(this::mapToDocumentAccessDTO)
                                .collect(Collectors.toList())
                );
    }

    private Uni<OtsDefinitionDTO> mapToDTO(OtsDefinition ots) {
        return Uni.combine().all().unis(
                userService.getUserName(ots.getAuthor()),
                userService.getUserName(ots.getLastModifier())
        ).asTuple().map(tuple -> {
            OtsDefinitionDTO dto = new OtsDefinitionDTO();
            dto.setId(ots.getId());
            dto.setAuthor(tuple.getItem1());
            dto.setRegDate(ots.getRegDate());
            dto.setLastModifier(tuple.getItem2());
            dto.setLastModifiedDate(ots.getLastModifiedDate());
            dto.setName(ots.getName());
            dto.setSlugName(ots.getSlugName());
            dto.setScriptId(ots.getScriptId());
            dto.setUserVariables(ots.getUserVariables());
            dto.setBrandId(ots.getBrandId());
            dto.setAgentId(ots.getAgentId());
            dto.setStatus(ots.getStatus());
            dto.setStatusHistory(ots.getStatusHistory());
            dto.setType(ots.getType());
            dto.setEstimatedDurationMin(ots.getEstimatedDurationMin());
            dto.setChatContext(ots.getChatContext());
            return dto;
        });
    }
}
