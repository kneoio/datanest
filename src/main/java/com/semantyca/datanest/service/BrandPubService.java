package com.semantyca.datanest.service;

import com.semantyca.core.model.cnst.LanguageCode;
import com.semantyca.core.model.cnst.LanguageTag;
import com.semantyca.core.model.user.IUser;
import com.semantyca.core.model.user.SuperUser;
import com.semantyca.core.service.UserService;
import com.semantyca.core.util.ColorUtil;
import com.semantyca.core.util.WebHelper;
import com.semantyca.datanest.config.DatanestConfig;
import com.semantyca.datanest.dto.StagePlaylistDTO;
import com.semantyca.datanest.dto.brand.*;
import com.semantyca.datanest.dto.script.*;
import com.semantyca.datanest.messaging.CommandPublisher;
import com.semantyca.datanest.messaging.MetricPublisher;
import com.semantyca.datanest.model.cnst.ScriptMode;
import com.semantyca.datanest.repository.BrandPubRepository;
import com.semantyca.datanest.repository.BrandRepository;
import com.semantyca.mixpla.dto.queue.command.CommandType;
import com.semantyca.mixpla.model.CustomAction;
import com.semantyca.mixpla.model.PlaylistRequest;
import com.semantyca.mixpla.model.Scene;
import com.semantyca.mixpla.model.ScenePrompt;
import com.semantyca.mixpla.model.Script;
import com.semantyca.mixpla.model.brand.Brand;
import com.semantyca.mixpla.model.brand.BrandScriptEntry;
import com.semantyca.mixpla.model.cnst.PlaylistItemType;
import com.semantyca.mixpla.model.cnst.SceneTimingMode;
import com.semantyca.mixpla.model.cnst.SourceType;
import com.semantyca.mixpla.model.cnst.WayOfSourcing;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Typed;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.net.MalformedURLException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

@ApplicationScoped
@Typed(BrandPubService.class)
public class BrandPubService extends BrandService {
    private static final Logger LOGGER = Logger.getLogger(BrandPubService.class);
    private final BrandPubRepository brandPubRepository;

    protected BrandPubService () {
        super();
        this.brandPubRepository = null;
    }

    @Inject
    public BrandPubService(
            UserService userService,
            ScriptService scriptService,
            SceneService sceneService,
            BrandRepository repository,
            DatanestConfig datanestConfig,
            MetricPublisher metricPublisher,
            CommandPublisher commandPublisher,
            BrandPubRepository brandPubRepository
    ) {
        super(userService, scriptService, sceneService, repository, datanestConfig, metricPublisher, commandPublisher);
        this.brandPubRepository = brandPubRepository;
    }

    public Uni<BrandDTO> upsert(String id, BrandDTO dto, IUser user, LanguageCode code) {
        boolean isNew = "new".equalsIgnoreCase(id) || id == null || id.isBlank();
        boolean isCustom = ScriptMode.CUSTOM.equals(dto.getScriptMode());
        return doUpsert(id, dto, isNew, isCustom, user)
                .invoke(saved -> commandPublisher.publishCommand(
                        CommandType.FLOW_RESTART,
                        "brand_saved",
                        Map.of("brandId", saved.getId().toString(), "slug", saved.getSlugName(), "savedBy", user.getUserName())
                ))
                .chain(this::mapToDTO);
    }

    private Uni<Brand> doUpsert(String id, BrandDTO dto, boolean isNew, boolean isCustom, IUser user) {
        if (isNew) {
            String slug = WebHelper.generateSlug(dto.getLocalizedName());
            Brand brand = super.buildEntity(dto, user, slug);
            brand.setPopularityRate(5);
            if (isCustom) {
                Script script = buildScript(slug,dto.getCustomScript().getTitle());
                String color = ColorUtil.generateContrastColorPair()[0];
                script.setColor(color);
                return brandPubRepository.insertBrandWithScript(brand, script, buildScenes(dto.getCustomScript()), List.of(), user)
                        .chain(brandId -> repository.findById(brandId, user, true));
            } else {
                return repository.insert(brand, List.of(), user);
            }
        } else {
            return repository.findById(UUID.fromString(id), user, false)
                    .chain(existingBrand -> {
                        String slug = existingBrand.getSlugName();
                        Brand brand = super.buildEntity(dto, user, slug);
                        if (isCustom) {
                            return resolveExistingCustomScriptId(existingBrand)
                                    .chain(existingScriptId -> {
                                        String color = ColorUtil.generateContrastColorPair()[0];
                                        Script script = buildScript(slug, dto.getCustomScript() != null ? dto.getCustomScript().getTitle() : null);
                                        script.setColor(color);
                                        List<Scene> scenes = buildScenes(dto.getCustomScript());
                                        if (existingScriptId == null) {
                                            return brandPubRepository.insertScriptAndUpdateBrand(existingBrand.getId(), brand, script, scenes, List.of(), user)
                                                    .chain(brandId -> repository.findById(brandId, user, true));
                                        }
                                        return brandPubRepository.updateBrandWithScript(existingBrand.getId(), existingScriptId, brand, script, scenes, List.of(), user)
                                                .chain(brandId -> repository.findById(brandId, user, true));
                                    });
                        } else {
                            return repository.update(existingBrand.getId(), brand, List.of(), user)
                                    .map(Brand::getId)
                                    .chain(brandId -> repository.findById(brandId, user, true));
                        }
                    });
        }
    }

    private Uni<UUID> resolveExistingCustomScriptId(Brand existing) {
        if (existing.getScripts() == null || existing.getScripts().isEmpty()) {
            return Uni.createFrom().nullItem();
        }
        return scriptService.getById(existing.getScripts().getFirst().getScriptId(), SuperUser.build())
                .map(script -> script.isCustom() ? script.getId() : null);
    }

    private Script buildScript(String slug, String title) {
        String name = title != null ? title : slug + "'s script";
        Script script = new Script();
        script.setName(name);
        script.setSlugName(WebHelper.generateSlug(name));
        script.setDescription("Custom script for " + slug);
        script.setCustom(true);
        script.setLanguageTag(LanguageTag.EN_US);
        script.setTimingMode(SceneTimingMode.ABSOLUTE_TIME);
        return script;
    }

    private List<Scene> buildScenes(CustomScriptDTO customScript) {
        if (customScript.getScenes() == null) {
            return List.of();
        }
        List<CustomSceneDTO> customScenes = customScript.getScenes();
        List<Scene> scenes = new ArrayList<>();
        for (int i = 0; i < customScenes.size(); i++) {
            scenes.add(buildScene(customScenes.get(i), i + 1));
        }
        return scenes;
    }

    private Scene buildScene(CustomSceneDTO customScene, int seqNum) {
        Scene scene = new Scene();
        scene.setTitle(customScene.getTitle() != null ? customScene.getTitle() : "Scene " + seqNum);
        scene.setSeqNum(seqNum);
        if (customScene.getStartTime() != null) {
            scene.setStartTime(List.of(customScene.getStartTime()));
        }
        scene.setTalkativity(customScene.getTalkativity());
        if (customScene.getIntroPrompts() != null) {
            scene.setIntroPrompts(customScene.getIntroPrompts().stream()
                    .map(this::mapToScenePrompt)
                    .collect(Collectors.toList()));
        }
        if (customScene.getActions() != null) {
            scene.setActions(customScene.getActions().stream()
                    .map(this::mapToCustomAction)
                    .collect(Collectors.toList()));
        }
        scene.setPlaylistRequest(mapToPlaylistRequest(customScene.getStagePlaylist()));
        return scene;
    }

    private ScenePrompt mapToScenePrompt(ScenePromptDTO dto) {
        ScenePrompt sp = new ScenePrompt();
        sp.setPromptId(dto.getPromptId());
        return sp;
    }

    private CustomAction mapToCustomAction(CustomActionDTO dto) {
        CustomAction action = new CustomAction();
        action.setName(dto.getName());
        action.setInstruction(dto.getInstruction());
        action.setContextVars(dto.getContextVars());
        return action;
    }

    private PlaylistRequest mapToPlaylistRequest(StagePlaylistDTO dto) {
        if (dto == null) {
            return null;
        }
        PlaylistRequest pr = new PlaylistRequest();
        pr.setSourcing(dto.getSourcing() != null ? WayOfSourcing.valueOf(dto.getSourcing()) : null);
        pr.setTitle(dto.getTitle());
        pr.setArtist(dto.getArtist());
        pr.setGenres(dto.getGenres());
        pr.setLabels(dto.getLabels());
        pr.setType(dto.getType() != null ? dto.getType().stream().map(PlaylistItemType::valueOf).toList() : null);
        pr.setSource(dto.getSource() != null ? dto.getSource().stream().map(SourceType::valueOf).toList() : null);
        pr.setSearchTerm(dto.getSearchTerm());
        pr.setSoundFragments(dto.getSoundFragments());
        return pr;
    }

    @Override
    Uni<BrandDTO> mapToDTO(Brand doc) {
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
                                    customScriptDTO.setTitle(customScript.getName());
                                    customScriptDTO.setScenes(sceneDTOs.stream()
                                            .map(this::toCustomSceneDTO)
                                            .collect(Collectors.toList()));
                                    dto.setCustomScript(customScriptDTO);
                                    return dto;
                                });
                    });
        });
    }
}
