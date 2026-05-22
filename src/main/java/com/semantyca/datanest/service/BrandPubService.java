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
import com.semantyca.datanest.dto.brand.BrandDTO;
import com.semantyca.datanest.dto.script.CustomActionDTO;
import com.semantyca.datanest.dto.script.CustomSceneDTO;
import com.semantyca.datanest.dto.script.CustomScriptDTO;
import com.semantyca.datanest.dto.script.ScenePromptDTO;
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
import com.semantyca.mixpla.model.cnst.PlaylistItemType;
import com.semantyca.mixpla.model.cnst.SceneTimingMode;
import com.semantyca.mixpla.model.cnst.SourceType;
import com.semantyca.mixpla.model.cnst.WayOfSourcing;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Typed;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

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

    protected BrandPubService() {}

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

    public Uni<BrandDTO> upsert(String slug, BrandDTO dto, IUser user, LanguageCode code) {
        boolean isNew = "new".equalsIgnoreCase(slug) || slug == null || slug.isBlank();
        boolean isCustom = ScriptMode.CUSTOM.equals(dto.getScriptMode());
        Uni<String> slugUni = isNew
                ? Uni.createFrom().item(WebHelper.generateSlug(dto.getLocalizedName()))
                : Uni.createFrom().item(slug);
        return slugUni
                .chain(resolvedSlug -> doUpsert(resolvedSlug, dto, isNew, isCustom, user))
                .invoke(saved -> commandPublisher.publishCommand(
                        CommandType.FLOW_RESTART,
                        "brand_saved",
                        Map.of("brandId", saved.getId().toString(), "slug", saved.getSlugName(), "savedBy", user.getUserName())
                ))
                .chain(this::mapToDTO);
    }

    private Uni<Brand> doUpsert(String slug, BrandDTO dto, boolean isNew, boolean isCustom, IUser user) {
        Brand brand = super.buildEntity(dto, user, slug);
        if (isNew) {
            brand.setPopularityRate(5);
            if (isCustom) {
                String color = ColorUtil.generateContrastColorPair()[0];
                Script script = buildScript(slug);
                script.setColor(color);
                return brandPubRepository.insertBrandWithScript(brand, script, buildScenes(dto.getCustomScript()), List.of(), user)
                        .chain(id -> repository.findById(id, user, true));
            } else {
                return repository.insert(brand, List.of(), user);
            }
        } else {
            return repository.getBySlugName(slug)
                    .chain(existing -> resolveExistingCustomScriptId(existing)
                            .chain(existingScriptId -> {
                                Uni<UUID> idUni;
                                if (isCustom) {
                                    String color = ColorUtil.generateContrastColorPair()[0];
                                    Script script = buildScript(slug);
                                    script.setColor(color);
                                    List<Scene> scenes = buildScenes(dto.getCustomScript());
                                    idUni = existingScriptId != null
                                            ? brandPubRepository.updateBrandWithScript(existing.getId(), existingScriptId, brand, script, scenes, List.of(), user)
                                            : brandPubRepository.insertScriptAndUpdateBrand(existing.getId(), brand, script, scenes, List.of(), user);
                                } else {
                                    idUni = existingScriptId != null
                                            ? brandPubRepository.archiveScriptAndUpdateBrand(existing.getId(), existingScriptId, brand, List.of(), user)
                                            : repository.update(existing.getId(), brand, List.of(), user).map(Brand::getId);
                                }
                                return idUni.chain(id -> repository.findById(id, user, true));
                            })
                    );
        }
    }

    private Uni<UUID> resolveExistingCustomScriptId(Brand existing) {
        if (existing.getScripts() == null || existing.getScripts().isEmpty()) {
            return Uni.createFrom().nullItem();
        }
        return scriptService.getById(existing.getScripts().getFirst().getScriptId(), SuperUser.build())
                .map(script -> script.isCustom() ? script.getId() : null);
    }

    private Script buildScript(String slug) {
        String name = slug + "'s script";
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
        scene.setTitle("Scene " + seqNum);
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
}
