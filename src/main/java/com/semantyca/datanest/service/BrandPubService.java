package com.semantyca.datanest.service;

import com.semantyca.core.model.cnst.LanguageTag;
import com.semantyca.core.model.user.IUser;
import com.semantyca.core.model.user.SuperUser;
import com.semantyca.core.repository.exception.DocumentHasNotFoundException;
import com.semantyca.core.util.WebHelper;
import com.semantyca.datanest.dto.script.SceneDTO;
import com.semantyca.datanest.dto.script.ScenePromptDTO;
import com.semantyca.datanest.dto.script.ScriptDTO;
import com.semantyca.datanest.dto.StagePlaylistDTO;
import com.semantyca.datanest.dto.script.CustomSceneDTO;
import com.semantyca.datanest.dto.script.CustomScriptDTO;
import com.semantyca.datanest.model.cnst.ScriptMode;
import com.semantyca.datanest.repository.BrandPubRepository;
import com.semantyca.datanest.repository.BrandRepository;
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
        ScriptDTO scriptDTO = isCustom ? buildScriptDTO(slug, customScriptDTO) : null;
        Script script = isCustom ? buildScriptEntity(scriptDTO) : null;
        List<Scene> scenes = isCustom ? buildSceneEntities(scriptDTO.getScenes()) : null;
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

    private ScriptDTO buildScriptDTO(String slug, CustomScriptDTO customScript) {
        ScriptDTO scriptDTO = new ScriptDTO();
        scriptDTO.setName(slug + "'s script");
        scriptDTO.setDescription("Custom script for " + slug);
        scriptDTO.setLanguageTag(LanguageTag.EN_US.tag());
        scriptDTO.setTimingMode(SceneTimingMode.ABSOLUTE_TIME.name());
        scriptDTO.setCustom(true);
        if (customScript.getScenes() != null) {
            List<SceneDTO> sceneDTOs = new ArrayList<>();
            List<CustomSceneDTO> customScenes = customScript.getScenes();
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
        sceneDTO.setTalkativity(customScene.getTalkativity());
        if (customScene.getActions() != null) {
            List<ScenePromptDTO> prompts = customScene.getActions().stream()
                    .map(a -> new ScenePromptDTO())
                    .collect(Collectors.toList());
            sceneDTO.setPrompts(prompts);
            if (customScene.getSongsMode() != null) {
            StagePlaylistDTO playlist = new StagePlaylistDTO();
            playlist.setSourcing(customScene.getSongsMode().name());
            sceneDTO.setStagePlaylist(playlist);
        }
        }
        return sceneDTO;
    }

    private Script buildScriptEntity(ScriptDTO dto) {
        Script script = new Script();
        script.setName(dto.getName());
        script.setSlugName(WebHelper.generateSlug(dto.getName()));
        script.setDescription(dto.getDescription());
        script.setCustom(dto.isCustom());
        script.setLanguageTag(LanguageTag.fromTag(dto.getLanguageTag()));
        script.setTimingMode(SceneTimingMode.valueOf(dto.getTimingMode()));
        script.setLabels(dto.getLabels());
        return script;
    }

    private List<Scene> buildSceneEntities(List<SceneDTO> dtos) {
        if (dtos == null) {
            return List.of();
        }
        return dtos.stream().map(this::buildSceneEntity).collect(Collectors.toList());
    }

    private Scene buildSceneEntity(SceneDTO dto) {
        Scene scene = new Scene();
        scene.setTitle(dto.getTitle());
        scene.setStartTime(dto.getStartTime());
        scene.setDurationSeconds(dto.getDurationSeconds());
        scene.setSeqNum(dto.getSeqNum());
        scene.setTalkativity(dto.getTalkativity());
        scene.setWeekdays(dto.getWeekdays());
        scene.setOneTimeRun(dto.isOneTimeRun());
        scene.setIntroPrompts(dto.getPrompts() != null
                ? dto.getPrompts().stream().map(p -> {
            ScenePrompt sp = new ScenePrompt();
            sp.setPromptId(p.getPromptId());
            sp.setRank(p.getRank());
            sp.setWeight(p.getWeight());
            sp.setActive(p.isActive());
            sp.setMandatory(p.isMandatory());
            return sp;
        }).collect(Collectors.toList())
                : List.of());
        scene.setPlaylistRequest(mapToPlaylistRequest(dto.getStagePlaylist()));
        return scene;
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
