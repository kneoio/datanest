package com.semantyca.datanest.service;

import com.semantyca.core.model.cnst.LanguageCode;
import com.semantyca.core.model.user.IUser;
import com.semantyca.core.service.UserService;
import com.semantyca.core.util.ColorUtil;
import com.semantyca.core.util.WebHelper;
import com.semantyca.datanest.config.DatanestConfig;
import com.semantyca.datanest.dto.PlaylistRequestDTO;
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
        LOGGER.infof("BrandPubService.upsert: id=%s isNew=%s isCustom=%s user=%s", id, isNew, isCustom, user.getUserName());
        return doUpsert(id, dto, isNew, isCustom, user)
                .invoke(saved -> {
                    LOGGER.infof("BrandPubService.upsert: brand saved id=%s slug=%s", saved.getId(), saved.getSlugName());
                    commandPublisher.publishCommand(
                            CommandType.FLOW_RESTART,
                            "brand_saved",
                            Map.of("brandId", saved.getId().toString(), "slug", saved.getSlugName(), "savedBy", user.getUserName())
                    );
                })
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
                            brand.setScriptIds(null);
                            return repository.update(existingBrand.getId(), brand, List.of(), user)
                                    .map(Brand::getId)
                                    .chain(brandId -> repository.findById(brandId, user, true));
                        }
                    });
        }
    }

    private Uni<UUID> resolveExistingCustomScriptId(Brand existing) {
        return Uni.createFrom().item(existing.getCustomScriptId());
    }

    private Script buildScript(String slug, String title) {
        String name = title != null ? title : slug + "'s script";
        Script script = new Script();
        script.setName(name);
        script.setSlugName(WebHelper.generateSlug(name));
        script.setDescription("Custom script for " + slug);
        script.setCustom(true);
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
        scene.setTitle(customScene.getName() != null ? customScene.getName() : "Scene " + seqNum);
        scene.setSeqNum(seqNum);
        if (customScene.getStartTime() != null) {
            scene.setStartTime(List.of(customScene.getStartTime()));
        }
        scene.setTalkativity(customScene.getTalkativity());
        scene.setAllowJingles(customScene.isAllowJingles());
        scene.setAllowAds(customScene.isAllowAds());
        if (customScene.getActions() != null) {
            List<CustomAction> customActions = customScene.getActions().stream()
                    .filter(a -> !"predefined".equals(a.getType()))
                    .map(this::mapToCustomAction)
                    .collect(Collectors.toList());
            if (!customActions.isEmpty()) scene.setActions(customActions);

            List<ScenePrompt> prompts = customScene.getActions().stream()
                    .filter(a -> "predefined".equals(a.getType()) && a.getActionId() != null)
                    .map(a -> { ScenePrompt sp = new ScenePrompt(); sp.setPromptId(a.getActionId()); return sp; })
                    .collect(Collectors.toList());
            if (!prompts.isEmpty()) scene.setIntroPrompts(prompts);
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
        return action;
    }

    private PlaylistRequest mapToPlaylistRequest(PlaylistRequestDTO dto) {
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
