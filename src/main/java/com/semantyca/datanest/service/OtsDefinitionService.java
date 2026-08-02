package com.semantyca.datanest.service;

import com.semantyca.core.dto.DocumentAccessDTO;
import com.semantyca.core.model.cnst.LanguageCode;
import com.semantyca.core.model.user.IUser;
import com.semantyca.core.model.user.SuperUser;
import com.semantyca.core.service.AbstractService;
import com.semantyca.core.service.UserService;
import com.semantyca.core.util.WebHelper;
import com.semantyca.datanest.dto.OtsDefinitionDTO;
import com.semantyca.datanest.dto.brand.mixdeck.OtsDefinitionMixdeckDTO;
import com.semantyca.datanest.dto.script.RelativeSceneDTO;
import com.semantyca.datanest.messaging.CommandPublisher;
import com.semantyca.datanest.repository.OtsDefinitionRepository;
import com.semantyca.mixpla.dto.queue.command.CommandType;
import com.semantyca.mixpla.model.Script;
import com.semantyca.mixpla.model.cnst.SceneTimingMode;
import com.semantyca.mixpla.model.filter.OtsDefinitionFilter;
import com.semantyca.mixpla.model.stream.OtsDefinition;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

@ApplicationScoped
public class OtsDefinitionService extends AbstractService<OtsDefinition, OtsDefinitionDTO> {
    private final OtsDefinitionRepository repository;
    private final ScriptService scriptService;
    private final BrandService brandService;
    private final AiAgentService aiAgentService;
    private final CommandPublisher commandPublisher;

    @Inject
    public OtsDefinitionService(UserService userService,
                                 OtsDefinitionRepository repository,
                                 ScriptService scriptService,
                                 BrandService brandService,
                                 AiAgentService aiAgentService,
                                 CommandPublisher commandPublisher) {
        super(userService);
        this.repository = repository;
        this.scriptService = scriptService;
        this.brandService = brandService;
        this.aiAgentService = aiAgentService;
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

    public Uni<List<OtsDefinitionMixdeckDTO>> getAllMixdeckDTO(final int limit, final int offset, final IUser user,
                                                               final OtsDefinitionFilter filter) {
        return repository.getAll(limit, offset, false, user, filter)
                .chain(list -> {
                    if (list.isEmpty()) {
                        return Uni.createFrom().item(List.of());
                    }
                    List<Uni<OtsDefinitionMixdeckDTO>> unis = list.stream()
                            .map(this::mapToMixdeckDTO)
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

    public Uni<OtsDefinitionMixdeckDTO> getMixdeckDTOBySlug(String slugName, IUser user) {
        return repository.findBySlugName(slugName, user, false).chain(this::mapToMixdeckDTO);
    }

    public Uni<OtsDefinitionDTO> getNewDTO(String scriptSlug, IUser user) {
        return scriptService.getIdBySlug(scriptSlug, user)
                .chain(scriptId -> scriptService.getById(scriptId, SuperUser.build()))
                .map(script -> {
                    OtsDefinitionDTO dto = new OtsDefinitionDTO();
                    dto.setScriptSlug(script.getSlugName());
                    dto.setName(script.getName());
                    dto.setColor(script.getColor());
                    dto.setRequiredVariables(script.getRequiredVariables());
                    return dto;
                });
    }

    public Uni<OtsDefinitionMixdeckDTO> getNewMixdeckDTO(String scriptSlug, IUser user) {
        return getNewDTO(scriptSlug, user).map(this::toMixdeckDTO);
    }

    @Override
    public Uni<OtsDefinitionDTO> upsert(String id, OtsDefinitionDTO dto, IUser user, LanguageCode code) {
        if (dto.getBrandSlug() == null && dto.getAgentSlug() == null) {
            return Uni.createFrom().failure(
                    new IllegalArgumentException("agentSlug is required when brandSlug is not set"));
        }
        if ("new".equalsIgnoreCase(id) || id == null || id.isBlank()) {
            return create(dto, user);
        }
        return update(UUID.fromString(id), dto, user);
    }

    /** Mixdeck upsert; path key is ots definition slug (not UUID). */
    public Uni<OtsDefinitionMixdeckDTO> upsertMixdeck(String slugName, OtsDefinitionMixdeckDTO mixdeckDto, IUser user) {
        OtsDefinitionDTO dto = fromMixdeckDTO(mixdeckDto);
        boolean isNew = slugName == null || slugName.isBlank() || "new".equalsIgnoreCase(slugName);
        if (isNew) {
            return upsert("new", dto, user, LanguageCode.en).map(this::toMixdeckDTO);
        }
        return repository.findBySlugName(slugName, user, false)
                .chain(existing -> upsert(existing.getId().toString(), dto, user, LanguageCode.en))
                .map(this::toMixdeckDTO);
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

    public Uni<Integer> deleteBySlug(String slugName, IUser user) {
        return repository.findBySlugName(slugName, user, false)
                .chain(ots -> delete(ots.getId().toString(), user));
    }

    private Uni<OtsDefinitionDTO> update(UUID id, OtsDefinitionDTO dto, IUser user) {
        return getScriptBySlug(dto.getScriptSlug(), user)
                .chain(script -> validateOtsCompatible(script)
                        .chain(v -> calculateEstimatedDurationMin(script.getId(), user))
                        .chain(estimatedDurationMin -> {
                            return buildEntity(dto, script.getId(), user)
                                    .chain(entity -> {
                                        entity.setName(script.getName());
                                        entity.setEstimatedDurationMin(estimatedDurationMin);
                                        entity.setChatContext(buildChatContext(script, dto.getUserVariables()));
                                        return repository.update(id, entity, user);
                                    });
                        }))
                .chain(this::mapToDTO);
    }

    private Uni<OtsDefinitionDTO> create(OtsDefinitionDTO dto, IUser user) {
        return getScriptBySlug(dto.getScriptSlug(), user)
                .chain(script -> validateOtsCompatible(script).chain(v -> generateName(script, dto.getUserVariables()))
                        .chain(name -> {
                            String slug = WebHelper.generateSlug(name + "-" + System.currentTimeMillis());
                            return repository.existsBySlug(slug)
                                    .chain(exists -> {
                                        if (exists) {
                                            return Uni.createFrom().failure(
                                                    new IllegalArgumentException("An ots definition with slug '" + slug + "' already exists"));
                                        }
                                        return calculateEstimatedDurationMin(script.getId(), user)
                                                .chain(estimatedDurationMin -> {
                                                    return buildEntity(dto, script.getId(), user)
                                                            .chain(entity -> {
                                                                entity.setName(name);
                                                                entity.setSlugName(slug);
                                                                entity.setEstimatedDurationMin(estimatedDurationMin);
                                                                entity.setChatContext(buildChatContext(script, dto.getUserVariables()));
                                                                return repository.insert(entity, user);
                                                            });
                                                });
                                    });
                        })
                )
                .chain(this::mapToDTO);
    }

    private Uni<Void> validateOtsCompatible(Script script) {
        if (script.getTimingMode() == SceneTimingMode.ABSOLUTE_TIME) {
            return Uni.createFrom().failure(new IllegalArgumentException(
                    "Script '" + script.getName() + "' uses ABSOLUTE_TIME timing and cannot be used for a one-time stream"));
        }
        return Uni.createFrom().voidItem();
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

    private Uni<Script> getScriptBySlug(String scriptSlug, IUser user) {
        return scriptService.getIdBySlug(scriptSlug, user)
                .chain(scriptId -> scriptService.getById(scriptId, SuperUser.build()));
    }

    private Uni<OtsDefinition> buildEntity(OtsDefinitionDTO dto, UUID scriptId, IUser user) {
        OtsDefinition entity = new OtsDefinition();
        entity.setName(dto.getName());
        entity.setScriptId(scriptId);
        entity.setUserVariables(dto.getUserVariables());
        entity.setType(dto.getType());

        Uni<Void> brandUni = dto.getBrandSlug() == null || dto.getBrandSlug().isBlank()
                ? Uni.createFrom().voidItem()
                : brandService.getBySlugNameForUser(dto.getBrandSlug(), user)
                        .invoke(brand -> entity.setBrandId(brand.getId()))
                        .replaceWithVoid();
        Uni<Void> agentUni = dto.getAgentSlug() == null || dto.getAgentSlug().isBlank()
                ? Uni.createFrom().voidItem()
                : aiAgentService.getIdBySlug(dto.getAgentSlug(), user)
                        .invoke(entity::setAgentId)
                        .replaceWithVoid();

        return Uni.combine().all().unis(brandUni, agentUni).discardItems().replaceWith(entity);
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
        return Uni.createFrom().item(script.getName());
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
                userService.getUserName(ots.getLastModifier()),
                scriptService.getSlugById(ots.getScriptId()),
                ots.getBrandId() == null
                        ? Uni.createFrom().item(Optional.<String>empty())
                        : brandService.getById(ots.getBrandId(), SuperUser.build())
                                .map(brand -> Optional.ofNullable(brand.getSlugName())),
                ots.getAgentId() == null
                        ? Uni.createFrom().item(Optional.<String>empty())
                        : aiAgentService.getSlugById(ots.getAgentId()).map(Optional::ofNullable)
        ).asTuple().map(tuple -> {
            OtsDefinitionDTO dto = new OtsDefinitionDTO();
            dto.setId(ots.getId());
            dto.setAuthor(tuple.getItem1());
            dto.setRegDate(ots.getRegDate());
            dto.setLastModifier(tuple.getItem2());
            dto.setLastModifiedDate(ots.getLastModifiedDate());
            dto.setName(ots.getName());
            dto.setSlugName(ots.getSlugName());
            dto.setScriptSlug(tuple.getItem3());
            dto.setUserVariables(ots.getUserVariables());
            dto.setBrandSlug(tuple.getItem4().orElse(null));
            dto.setAgentSlug(tuple.getItem5().orElse(null));
            dto.setStatus(ots.getStatus());
            dto.setStatusHistory(ots.getStatusHistory());
            dto.setType(ots.getType());
            dto.setEstimatedDurationMin(ots.getEstimatedDurationMin());
            dto.setChatContext(ots.getChatContext());
            return dto;
        });
    }

    private Uni<OtsDefinitionMixdeckDTO> mapToMixdeckDTO(OtsDefinition ots) {
        return mapToDTO(ots).map(this::toMixdeckDTO);
    }

    private OtsDefinitionMixdeckDTO toMixdeckDTO(OtsDefinitionDTO src) {
        OtsDefinitionMixdeckDTO dto = new OtsDefinitionMixdeckDTO();
        dto.setAuthor(src.getAuthor());
        dto.setRegDate(src.getRegDate());
        dto.setLastModifier(src.getLastModifier());
        dto.setLastModifiedDate(src.getLastModifiedDate());
        dto.setName(src.getName());
        dto.setSlugName(src.getSlugName());
        dto.setScriptSlug(src.getScriptSlug());
        dto.setUserVariables(src.getUserVariables());
        dto.setBrandSlug(src.getBrandSlug());
        dto.setAgentSlug(src.getAgentSlug());
        dto.setStatus(src.getStatus());
        dto.setStatusHistory(src.getStatusHistory());
        dto.setType(src.getType());
        dto.setEstimatedDurationMin(src.getEstimatedDurationMin());
        dto.setChatContext(src.getChatContext());
        dto.setColor(src.getColor());
        dto.setRequiredVariables(src.getRequiredVariables());
        return dto;
    }

    private OtsDefinitionDTO fromMixdeckDTO(OtsDefinitionMixdeckDTO src) {
        OtsDefinitionDTO dto = new OtsDefinitionDTO();
        dto.setName(src.getName());
        dto.setSlugName(src.getSlugName());
        dto.setScriptSlug(src.getScriptSlug());
        dto.setUserVariables(src.getUserVariables());
        dto.setBrandSlug(src.getBrandSlug());
        dto.setAgentSlug(src.getAgentSlug());
        dto.setStatus(src.getStatus());
        dto.setStatusHistory(src.getStatusHistory());
        dto.setType(src.getType());
        dto.setEstimatedDurationMin(src.getEstimatedDurationMin());
        dto.setChatContext(src.getChatContext());
        dto.setColor(src.getColor());
        dto.setRequiredVariables(src.getRequiredVariables());
        return dto;
    }
}
