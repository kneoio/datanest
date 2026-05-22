package com.semantyca.datanest.service;

import com.semantyca.core.model.cnst.LanguageTag;
import com.semantyca.core.model.user.IUser;
import com.semantyca.core.model.user.SuperUser;
import com.semantyca.core.repository.exception.DocumentHasNotFoundException;
import com.semantyca.core.util.WebHelper;
import com.semantyca.datanest.dto.StagePlaylistDTO;
import com.semantyca.datanest.dto.script.CustomActionDTO;
import com.semantyca.datanest.dto.script.CustomSceneDTO;
import com.semantyca.datanest.dto.script.CustomScriptDTO;
import com.semantyca.datanest.dto.script.ScenePromptDTO;
import com.semantyca.datanest.model.cnst.ScriptMode;
import com.semantyca.datanest.repository.BrandPubRepository;
import com.semantyca.datanest.repository.BrandRepository;
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
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@ApplicationScoped
public class BrandPubService {

    private static final Logger LOGGER = Logger.getLogger(BrandPubService.class);

    private final BrandRepository repository;
    private final BrandPubRepository brandPubRepository;
    private final ScriptService scriptService;

    @Inject
    public BrandPubService(
            BrandRepository repository,
            BrandPubRepository brandPubRepository,
            ScriptService scriptService
    ) {
        this.repository = repository;
        this.brandPubRepository = brandPubRepository;
        this.scriptService = scriptService;
    }

    public Uni<Brand> upsert(String slug, Brand brand, ScriptMode scriptMode, CustomScriptDTO customScriptDTO, IUser user) {
        boolean isCustom = ScriptMode.CUSTOM.equals(scriptMode);
        Script script = isCustom ? buildScript(slug) : null;
        List<Scene> scenes = isCustom ? buildScenes(customScriptDTO) : null;
        assert script != null;
        script.setColor("#47C53FFF");
        Uni<UUID> brandIdUni = repository.getBySlugName(slug)
                .chain(existing -> resolveExistingCustomScriptId(existing)
                        .chain(existingScriptId -> {
                            if (isCustom) {
                                if (existingScriptId != null) {
                                    return brandPubRepository.updateBrandWithScript(existing.getId(), existingScriptId, brand, script, scenes, List.of(), user);
                                } else {
                                    return brandPubRepository.insertScriptAndUpdateBrand(existing.getId(), brand, script, scenes, List.of(), user);
                                }
                            } else {
                                if (existingScriptId != null) {
                                    return brandPubRepository.archiveScriptAndUpdateBrand(existing.getId(), existingScriptId, brand, List.of(), user);
                                } else {
                                    return repository.update(existing.getId(), brand, List.of(), user).map(Brand::getId);
                                }
                            }
                        })
                )
                .onFailure(DocumentHasNotFoundException.class).recoverWithUni(() -> {
                    brand.setPopularityRate(5);
                    if (isCustom) {
                        return brandPubRepository.insertBrandWithScript(brand, script, scenes, List.of(), user);
                    } else {
                        return repository.insert(brand, List.of(), user).map(Brand::getId);
                    }
                });

        return brandIdUni.chain(brandId -> repository.findById(brandId, user, true));
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
        sp.setRank(dto.getRank());
        sp.setWeight(dto.getWeight());
        sp.setActive(dto.isActive());
        sp.setMandatory(dto.isMandatory());
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
