package com.semantyca.datanest.service;

import com.semantyca.core.dto.DocumentAccessDTO;
import com.semantyca.core.model.cnst.LanguageCode;
import com.semantyca.core.repository.exception.DocumentHasNotFoundException;
import com.semantyca.core.model.user.IUser;
import com.semantyca.core.model.user.SuperUser;
import com.semantyca.core.service.AbstractService;
import com.semantyca.core.service.UserService;
import com.semantyca.core.util.WebHelper;
import com.semantyca.datanest.config.DatanestConfig;
import com.semantyca.core.dto.rls.RlsActionDTO;
import com.semantyca.datanest.dto.SceneDTO;
import com.semantyca.datanest.dto.ScenePromptDTO;
import com.semantyca.datanest.dto.ScriptDTO;
import com.semantyca.datanest.dto.StagePlaylistDTO;
import com.semantyca.datanest.dto.radiostation.*;
import com.semantyca.datanest.model.cnst.ScriptMode;
import com.semantyca.mixpla.model.Script;
import com.semantyca.mixpla.model.cnst.WayOfSourcing;
import com.semantyca.datanest.messaging.CommandPublisher;
import com.semantyca.datanest.messaging.MetricPublisher;
import com.semantyca.datanest.repository.BrandRepository;
import com.semantyca.mixpla.dto.queue.command.CommandType;
import com.semantyca.mixpla.dto.queue.metric.MetricEventType;
import com.semantyca.mixpla.dto.queue.metric.ProcessType;
import com.semantyca.mixpla.model.brand.*;
import com.semantyca.mixpla.model.cnst.ManagedBy;
import com.semantyca.mixpla.model.filter.BrandFilter;
import com.semantyca.officeframe.model.cnst.CountryCode;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.net.URI;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

@ApplicationScoped
public class BrandService extends AbstractService<Brand, BrandDTO> {
    private static final Logger LOGGER = Logger.getLogger(BrandService.class);

    private final BrandRepository repository;

    private final DatanestConfig datanestConfig;

    ScriptService scriptService;

    SceneService sceneService;

    MetricPublisher metricPublisher;

    CommandPublisher commandPublisher;

    @Inject
    public BrandService(
            UserService userService,
            ScriptService scriptService,
            SceneService sceneService,
            BrandRepository repository,
            DatanestConfig datanestConfig,
            MetricPublisher metricPublisher,
            CommandPublisher commandPublisher
    ) {
        super(userService);
        this.scriptService = scriptService;
        this.sceneService = sceneService;
        this.repository = repository;
        this.datanestConfig = datanestConfig;
        this.metricPublisher = metricPublisher;
        this.commandPublisher = commandPublisher;
    }

    public Uni<List<BrandDTO>> getAllDTO(final int limit, final int offset, final IUser user, final BrandFilter filter) {
        assert repository != null;
        return repository.getAll(limit, offset, false, user, filter)
                .chain(list -> {
                    if (list.isEmpty()) {
                        return Uni.createFrom().item(List.of());
                    } else {
                        List<Uni<BrandDTO>> unis = list.stream()
                                .map(this::mapToDTO)
                                .collect(Collectors.toList());
                        return Uni.join().all(unis).andFailFast();
                    }
                });
    }

    public Uni<Integer> getAllCount(final IUser user, final BrandFilter filter) {
        assert repository != null;
        return repository.getAllCount(user, false, filter);
    }

    public Uni<List<Brand>> getAll(final int limit, final int offset) {
        return repository.getAll(limit, offset, false, SuperUser.build(), null);
    }

    public Uni<List<Brand>> getAll(final int limit, final int offset, IUser user) {
        return repository.getAll(limit, offset, false, user, null);
    }

    public Uni<Brand> getById(UUID id, IUser user) {
        return repository.findById(id, user, true);
    }

    public Uni<Brand> getBySlugNameForUser(String name, IUser user) {
        return repository.getBySlugName(name, user, false);
    }

    public Uni<Brand> getBySlugName(String name) {
        return repository.getBySlugName(name)
                .chain(brand -> {
                    if (brand == null) {
                        return Uni.createFrom().nullItem();
                    }
                    return scriptService.getAllScriptsForBrandWithScenes(brand.getId(), SuperUser.build())
                            .map(brandScripts -> {
                                List<BrandScriptEntry> entries = brandScripts.stream()
                                        .map(bs -> new BrandScriptEntry(
                                                bs.getScript().getId(),
                                                bs.getUserVariables()
                                        ))
                                        .collect(Collectors.toList());
                                brand.setScripts(entries);
                                return brand;
                            });
                });
    }

    @Override
    public Uni<Integer> delete(String id, IUser user) {
        assert repository != null;
        return repository.delete(UUID.fromString(id), user);
    }

    @Override
    public Uni<BrandDTO> getDTO(UUID id, IUser user, LanguageCode language) {
        assert repository != null;
        return repository.findById(id, user, false).chain(this::mapToDTO);
    }

    public Uni<BrandDTO> upsert(String id, BrandDTO dto, IUser user, LanguageCode code) {
        assert repository != null;
        LOGGER.infof("Upserting radio station with DTO scripts: %s", dto.getScripts());
        Brand entity = buildEntity(dto, user);
        LOGGER.infof("Built entity with scripts: %s", entity.getScripts());

        List<RlsActionDTO> rlsActions = dto.getRlsActions() != null ? dto.getRlsActions() : List.of();

        Uni<Brand> saveOperation;
        if ("new".equalsIgnoreCase(id) || id == null || id.isBlank()) {
            entity.setPopularityRate(5);
            saveOperation = repository.insert(entity, rlsActions, user);
        } else {
            saveOperation = repository.update(UUID.fromString(id), entity, rlsActions, user);
        }

        return saveOperation
                .invoke(saved -> commandPublisher.publishCommand(
                        CommandType.FLOW_RESTART,
                        "brand_saved",
                        Map.of("brandId", saved.getId().toString(), "slug", saved.getSlugName(), "savedBy", user.getUserName())
                ))
                .chain(this::mapToDTO);
    }

    public Uni<BrandDTO> upsertBySlug(String slug, BrandDTO dto, IUser user, LanguageCode code) {
        assert repository != null;
        Brand entity = buildEntity(dto, user);
        entity.setSlugName(slug);
        List<RlsActionDTO> rlsActions = dto.getRlsActions() != null ? dto.getRlsActions() : List.of();

        Uni<Brand> saveOperation;
        if (ScriptMode.CUSTOM.equals(dto.getScriptMode())) {
            saveOperation = upsertWithCustomScript(slug, dto, entity, rlsActions, user);
        } else {
            saveOperation = repository.getBySlugName(slug)
                    .chain(existing -> repository.update(existing.getId(), entity, rlsActions, user))
                    .onFailure(DocumentHasNotFoundException.class).recoverWithUni(() -> {
                        entity.setPopularityRate(5);
                        return repository.insert(entity, rlsActions, user);
                    });
        }

        return saveOperation
                .invoke(saved -> commandPublisher.publishCommand(
                        CommandType.FLOW_RESTART,
                        "brand_saved",
                        Map.of("brandId", saved.getId().toString(), "slug", saved.getSlugName(), "savedBy", user.getUserName())
                ))
                .chain(this::mapToDTO);
    }

    private Uni<Brand> upsertWithCustomScript(String slug, BrandDTO dto, Brand entity, List<RlsActionDTO> rlsActions, IUser user) {
        ScriptDTO scriptDTO = buildCustomScriptDTO(slug, dto);
        return repository.getBySlugName(slug)
                .chain(existing -> {
                    String scriptId = existing.getScripts() != null && !existing.getScripts().isEmpty()
                            ? existing.getScripts().get(0).getScriptId().toString()
                            : "new";
                    return scriptService.upsert(scriptId, scriptDTO, user)
                            .chain(script -> {
                                entity.setScripts(List.of(new BrandScriptEntry(script.getId(), Map.of())));
                                return repository.update(existing.getId(), entity, rlsActions, user);
                            });
                })
                .onFailure(DocumentHasNotFoundException.class).recoverWithUni(() -> {
                    entity.setPopularityRate(5);
                    return scriptService.upsert("new", scriptDTO, user)
                            .chain(script -> {
                                entity.setScripts(List.of(new BrandScriptEntry(script.getId(), Map.of())));
                                return repository.insert(entity, rlsActions, user);
                            });
                });
    }

    private ScriptDTO buildCustomScriptDTO(String slug, BrandDTO dto) {
        String brandName = dto.getLocalizedName().getOrDefault(LanguageCode.en, slug);
        ScriptDTO scriptDTO = new ScriptDTO();
        scriptDTO.setName(brandName + "'s script");
        scriptDTO.setDescription("Custom script for " + brandName);
        scriptDTO.setLanguageTag("en-US");
        scriptDTO.setTimingMode("ABSOLUTE_TIME");
        scriptDTO.setCustom(true);
        if (dto.getCustomScript().getScenes() != null) {
            List<SceneDTO> sceneDTOs = new ArrayList<>();
            List<CustomSceneDTO> customScenes = dto.getCustomScript().getScenes();
            for (int i = 0; i < customScenes.size(); i++) {
                sceneDTOs.add(convertCustomScene(customScenes.get(i), i + 1));
            }
            scriptDTO.setScenes(sceneDTOs);
        }
        return scriptDTO;
    }

    private SceneDTO convertCustomScene(CustomSceneDTO customScene, int seqNum) {
        SceneDTO sceneDTO = new SceneDTO();
        sceneDTO.setTitle("Scene " + seqNum);
        sceneDTO.setSeqNum(seqNum);
        if (customScene.getStartTime() != null) {
            sceneDTO.setStartTime(List.of(customScene.getStartTime()));
        }
        sceneDTO.setTalkativity(customScene.getTalkActivity());
        if (customScene.getActions() != null) {
            List<ScenePromptDTO> prompts = customScene.getActions().stream()
                    .filter(a -> a.isPredefined() && a.getPromptId() != null)
                    .map(a -> new ScenePromptDTO(a.getPromptId(), true, false, 0, BigDecimal.valueOf(0.5)))
                    .collect(Collectors.toList());
            if (!prompts.isEmpty()) {
                sceneDTO.setPrompts(prompts);
            }
            customScene.getActions().stream()
                    .filter(a -> a.getSongsMode() != null)
                    .findFirst()
                    .ifPresent(a -> {
                        StagePlaylistDTO playlist = new StagePlaylistDTO();
                        playlist.setSourcing(a.getSongsMode().name());
                        sceneDTO.setStagePlaylist(playlist);
                    });
        }
        return sceneDTO;
    }

    public Uni<List<BrandDTO>> getAllOpenForSubmissionDTO(int limit, int offset, IUser user) {
        return repository.getAllOpenForSubmission(limit, offset, user.getId())
                .chain(list -> {
                    if (list.isEmpty()) {
                        return Uni.createFrom().item(List.of());
                    }
                    List<Uni<BrandDTO>> unis = list.stream()
                            .map(this::mapToDTO)
                            .collect(Collectors.toList());
                    return Uni.join().all(unis).andFailFast();
                });
    }

    public Uni<Integer> getAllOpenForSubmissionCount(IUser user) {
        return repository.getAllOpenForSubmissionCount(user.getId());
    }

    public Uni<Integer> archive(String id, IUser user) {
        assert repository != null;
        return repository.findById(UUID.fromString(id), user, false)
                .chain(radioStation -> {
                    return repository.archive(UUID.fromString(id), user);
                });
    }

    public Uni<Integer> archive(UUID id) {
        assert repository != null;
        return repository.archive(id, SuperUser.build());
    }

    public Uni<Integer> closeBrand(String id, IUser user) {
        assert repository != null;
        UUID brandId = UUID.fromString(id);
        return repository.findById(brandId, user, false)
                .chain(brand -> repository.closeBrand(brandId, user)
                        .invoke(count -> {
                            if (count > 0) {
                                metricPublisher.publishMetric(
                                        brand.getSlugName(),
                                        MetricEventType.WARNING,
                                        ProcessType.INDEPENDENT,
                                        "brand_closed",
                                        Map.of("brandId", brandId.toString(), "closedBy", user.getUserName())
                                );
                            }
                        })
                );
    }

    private Uni<BrandDTO> mapToDTO(Brand doc) {
        return Uni.combine().all().unis(
                userService.getUserName(doc.getAuthor()),
                userService.getUserName(doc.getLastModifier()),
                repository.getScriptEntriesForBrand(doc.getId())
        ).asTuple().chain(tuple -> {
            BrandDTO dto = new BrandDTO();
            dto.setId(doc.getId());
            dto.setAuthor(tuple.getItem1());
            dto.setRegDate(doc.getRegDate());
            dto.setLastModifier(tuple.getItem2());
            dto.setLastModifiedDate(doc.getLastModifiedDate());
            dto.setLocalizedName(doc.getLocalizedName());
            dto.setCountry(doc.getCountry() != null ? doc.getCountry().name() : null);
            dto.setColor(doc.getColor());
            dto.setTimeZone(doc.getTimeZone().getId());
            dto.setDescription(doc.getDescription());
            dto.setTitleFont(doc.getTitleFont());
            dto.setSlugName(doc.getSlugName());
            dto.setBitRate(doc.getBitRate());
            dto.setAiAgentId(doc.getAiAgentId());
            dto.setProfileId(doc.getProfileId());
            dto.setOneTimeStreamPolicy(doc.getOneTimeStreamPolicy());
            dto.setSubmissionPolicy(doc.getSubmissionPolicy());
            dto.setMessagingPolicy(doc.getMessagingPolicy());
            dto.setIsTemporary(doc.getIsTemporary());
            dto.setPublicBrand(doc.getPublicBrand());
            dto.setPopularityRate(doc.getPopularityRate());

            if (doc.getAiOverriding() != null) {
                AiOverridingDTO aiDto = new AiOverridingDTO();
                aiDto.setName(doc.getAiOverriding().getName());
                aiDto.setPrompt(doc.getAiOverriding().getPrompt());
                aiDto.setPrimaryVoice(doc.getAiOverriding().getPrimaryVoice());
                dto.setAiOverriding(aiDto);
                dto.setAiOverridingEnabled(true);
            } else {
                dto.setAiOverridingEnabled(false);
            }

            if (doc.getProfileOverriding() != null) {
                ProfileOverridingDTO profileDto = new ProfileOverridingDTO();
                profileDto.setName(doc.getProfileOverriding().getName());
                profileDto.setDescription(doc.getProfileOverriding().getDescription());
                dto.setProfileOverriding(profileDto);
                dto.setProfileOverridingEnabled(true);
            } else {
                dto.setProfileOverridingEnabled(false);
            }

            try {
                dto.setHlsUrl(URI.create(datanestConfig.getHost() + "/live/" + dto.getSlugName() + "/stream.m3u8").toURL());
                //https://mixpla.online/aivox/aye-aye-s-ear/master.m3u8
                dto.setIceCastUrl(URI.create(datanestConfig.getHost() + "/" + dto.getSlugName() + "/radio/icecast").toURL());
                dto.setMp3Url(URI.create(datanestConfig.getHost() + "/" + dto.getSlugName() + "/radio/stream.mp3").toURL());
                dto.setMixplaUrl(URI.create("https://mixpla.online/" + dto.getSlugName()).toURL());
            } catch (MalformedURLException e) {
                throw new RuntimeException(e);
            }
            List<BrandScriptEntryDTO> scriptDTOs = tuple.getItem3().stream()
                    .map(entry -> {
                        BrandScriptEntryDTO scriptDTO = new BrandScriptEntryDTO();
                        scriptDTO.setScriptId(entry.getScriptId());
                        scriptDTO.setUserVariables(entry.getUserVariables());
                        return scriptDTO;
                    })
                    .collect(Collectors.toList());
            dto.setScripts(scriptDTOs);
            if (doc.getOwner() != null) {
                OwnerDTO ownerDTO = new OwnerDTO();
                ownerDTO.setUserId(doc.getOwner().getUserId());
                ownerDTO.setName(doc.getOwner().getName());
                ownerDTO.setEmail(doc.getOwner().getEmail());
                dto.setOwner(ownerDTO);
            }
            dto.setLabels(doc.getLabels());
            dto.setGenres(doc.getGenres());

            List<BrandScriptEntry> entries = tuple.getItem3();
            if (entries.isEmpty()) {
                dto.setScriptMode(ScriptMode.PREDEFINED);
                return Uni.createFrom().item(dto);
            }

            List<Uni<Script>> scriptUnis = entries.stream()
                    .map(e -> scriptService.getById(e.getScriptId(), SuperUser.build()))
                    .collect(Collectors.toList());

            return Uni.join().all(scriptUnis).andFailFast()
                    .chain(scripts -> {
                        Script customScript = scripts.stream()
                                .filter(Script::isCustom)
                                .findFirst()
                                .orElse(null);
                        if (customScript == null) {
                            dto.setScriptMode(ScriptMode.PREDEFINED);
                            return Uni.createFrom().item(dto);
                        }
                        dto.setScriptMode(ScriptMode.CUSTOM);
                        return sceneService.getAllByScript(customScript.getId(), 1000, 0, SuperUser.build())
                                .map(sceneDTOs -> {
                                    CustomScriptDTO customScriptDTO = new CustomScriptDTO();
                                    customScriptDTO.setScenes(sceneDTOs.stream()
                                            .map(this::toCustomSceneDTO)
                                            .collect(Collectors.toList()));
                                    dto.setCustomScript(customScriptDTO);
                                    return dto;
                                });
                    });
        });
    }

    private CustomSceneDTO toCustomSceneDTO(SceneDTO scene) {
        CustomSceneDTO customScene = new CustomSceneDTO();
        if (scene.getStartTime() != null && !scene.getStartTime().isEmpty()) {
            customScene.setStartTime(scene.getStartTime().get(0));
        }
        customScene.setTalkActivity((int) scene.getTalkativity());
        List<SceneActionDTO> actions = new ArrayList<>();
        if (scene.getPrompts() != null) {
            scene.getPrompts().stream()
                    .filter(p -> p.getPromptId() != null)
                    .map(p -> {
                        SceneActionDTO action = new SceneActionDTO();
                        action.setPredefined(true);
                        action.setPromptId(p.getPromptId());
                        return action;
                    })
                    .forEach(actions::add);
        }
        if (scene.getStagePlaylist() != null && scene.getStagePlaylist().getSourcing() != null) {
            SceneActionDTO songAction = new SceneActionDTO();
            try {
                songAction.setSongsMode(WayOfSourcing.valueOf(scene.getStagePlaylist().getSourcing()));
            } catch (IllegalArgumentException ignored) {
            }
            actions.add(songAction);
        }
        customScene.setActions(actions.isEmpty() ? null : actions);
        return customScene;
    }

    private Brand buildEntity(BrandDTO dto, IUser user) {
        Brand doc = new Brand();
        doc.setLocalizedName(dto.getLocalizedName());
        doc.setCountry(CountryCode.fromString(dto.getCountry()));
        doc.setIsTemporary(dto.getIsTemporary() != null ? dto.getIsTemporary() : 0);
        doc.setPublicBrand(dto.getPublicBrand());
        doc.setManagedBy(ManagedBy.MIX);
        doc.setColor(dto.getColor());
        doc.setDescription(dto.getDescription());
        doc.setTitleFont(dto.getTitleFont());
        doc.setTimeZone(ZoneId.of(dto.getTimeZone()));
        doc.setSlugName(WebHelper.generateSlug(dto.getLocalizedName()));
        doc.setBitRate(dto.getBitRate());
        doc.setAiAgentId(dto.getAiAgentId());
        doc.setProfileId(dto.getProfileId());
        doc.setOneTimeStreamPolicy(dto.getOneTimeStreamPolicy());
        doc.setSubmissionPolicy(dto.getSubmissionPolicy());
        doc.setMessagingPolicy(dto.getMessagingPolicy());
        //doc.setPopularityRate(dto.getPopularityRate());  //cannot be changed from UI

        if (dto.getAiOverriding() != null) {
            //TODO should be validation
            AiOverriding ai = new AiOverriding();
            ai.setName(dto.getAiOverriding().getName());
            ai.setPrompt(dto.getAiOverriding().getPrompt());
            ai.setPrimaryVoice(dto.getAiOverriding().getPrimaryVoice());
            doc.setAiOverriding(ai);
        }

        if (dto.getProfileOverriding() != null) {
            ProfileOverriding profile = new ProfileOverriding();
            profile.setName(dto.getProfileOverriding().getName());
            profile.setDescription(dto.getProfileOverriding().getDescription());
            doc.setProfileOverriding(profile);
        }

        if (dto.getScripts() != null) {
            List<BrandScriptEntry> scriptEntries = dto.getScripts().stream()
                    .map(e -> new BrandScriptEntry(e.getScriptId(), e.getUserVariables()))
                    .collect(Collectors.toList());
            doc.setScripts(scriptEntries);
        }

        if (dto.getOwner() != null) {
            Owner owner = new Owner();
            owner.setUserId(dto.getOwner().getUserId() > 0 ? dto.getOwner().getUserId() : user.getId());
            owner.setName(dto.getOwner().getName());
            owner.setEmail(dto.getOwner().getEmail());
            doc.setOwner(owner);
        }

        if (dto.getLabels() != null) {
            doc.setLabels(dto.getLabels());
        }

        if (dto.getGenres() != null) {
            doc.setGenres(dto.getGenres());
        }

        return doc;
    }

    public Uni<List<DocumentAccessDTO>> getDocumentAccess(UUID documentId, IUser user) {
        assert repository != null;
        return repository.getDocumentAccessInfo(documentId, user)
                .onItem().transform(accessInfoList ->
                        accessInfoList.stream()
                                .map(this::mapToDocumentAccessDTO)
                                .collect(Collectors.toList())
                );
    }
}

