package com.semantyca.datanest.service;

import com.semantyca.core.model.cnst.LanguageCode;
import com.semantyca.core.model.user.IUser;
import com.semantyca.core.service.UserService;
import com.semantyca.core.util.ColorUtil;
import com.semantyca.core.util.WebHelper;
import com.semantyca.datanest.config.DatanestConfig;
import com.semantyca.datanest.dto.PlaylistRequestDTO;
import com.semantyca.datanest.dto.brand.BrandDTO;
import com.semantyca.datanest.dto.brand.mixdeck.BrandScriptEntryMixdeckDTO;
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
import com.semantyca.mixpla.repository.UserSubscriptionRepository;
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
import com.semantyca.officeframe.service.LabelService;
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
            AiAgentService aiAgentService,
            ProfileService profileService,
            LabelService labelService,
            SceneService sceneService,
            BrandRepository repository,
            DatanestConfig datanestConfig,
            MetricPublisher metricPublisher,
            CommandPublisher commandPublisher,
            UserSubscriptionRepository userSubscriptionRepository,
            BrandPubRepository brandPubRepository
    ) {
        super(userService, scriptService, aiAgentService, profileService, labelService, sceneService, repository, datanestConfig, metricPublisher, commandPublisher, userSubscriptionRepository);
        this.brandPubRepository = brandPubRepository;
    }

    public Uni<com.semantyca.datanest.dto.brand.mixdeck.BrandMixdeckDTO> upsert(
            String id, com.semantyca.datanest.dto.brand.mixdeck.BrandMixdeckDTO dto, IUser user, LanguageCode code) {
        boolean isNew = "new".equalsIgnoreCase(id) || id == null || id.isBlank();
        boolean isCustom = ScriptMode.CUSTOM.equals(dto.getScriptMode());
        LOGGER.infof("BrandPubService.upsert: id=%s isNew=%s isCustom=%s user=%s", id, isNew, isCustom, user.getUserName());
        return toAdminDto(dto, user)
                .chain(adminDto -> resolveOwnerUserIds(adminDto)
                        .chain(resolvedDto -> doUpsert(id, resolvedDto, dto, isNew, isCustom, user)))
                .invoke(saved -> {
                    LOGGER.infof("BrandPubService.upsert: brand saved id=%s slug=%s", saved.getId(), saved.getSlugName());
                    commandPublisher.publishCommand(
                            CommandType.FLOW_RESTART,
                            "brand_saved",
                            Map.of("brandId", saved.getId().toString(), "slug", saved.getSlugName(), "savedBy", user.getUserName())
                    );
                })
                .chain(this::mapToMixdeckDTO);
    }

    private Uni<Brand> doUpsert(String id, BrandDTO adminDto, com.semantyca.datanest.dto.brand.mixdeck.BrandMixdeckDTO mixdeckDto,
                                boolean isNew, boolean isCustom, IUser user) {
        if (isNew) {
            String slug = WebHelper.generateSlug(adminDto.getLocalizedName());
            Brand brand = super.buildEntity(adminDto, user, slug);
            brand.setPopularityRate(5);
            return resolveScriptEntries(mixdeckDto, user)
                    .chain(entries -> {
                        brand.setScriptIds(entries);
                        if (isCustom) {
                            Script script = buildScript(slug, mixdeckDto.getCustomScript().getTitle());
                            String color = ColorUtil.generateContrastColorPair()[0];
                            script.setColor(color);
                            return brandPubRepository.insertBrandWithScript(brand, script, buildScenes(mixdeckDto.getCustomScript()), List.of(), user)
                                    .chain(brandId -> repository.findById(brandId, user, true));
                        } else {
                            return repository.insert(brand, List.of(), user);
                        }
                    });
        } else {
            return repository.getBySlugName(id, user, false)
                    .chain(existingBrand -> {
                        String slug = existingBrand.getSlugName();
                        Brand brand = super.buildEntity(adminDto, user, slug, existingBrand.getOwner());
                        return resolveScriptEntries(mixdeckDto, user)
                                .chain(entries -> {
                                    brand.setScriptIds(entries);
                                    if (isCustom) {
                                        return resolveExistingCustomScriptId(existingBrand)
                                                .chain(existingScriptId -> {
                                                    String color = ColorUtil.generateContrastColorPair()[0];
                                                    Script script = buildScript(slug, mixdeckDto.getCustomScript() != null ? mixdeckDto.getCustomScript().getTitle() : null);
                                                    script.setColor(color);
                                                    List<Scene> scenes = buildScenes(mixdeckDto.getCustomScript());
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
                    });
        }
    }

    /** Maps Mixdeck DTO → admin DTO for shared build/owner logic. Slugs/identifiers resolved to UUIDs. */
    private Uni<BrandDTO> toAdminDto(com.semantyca.datanest.dto.brand.mixdeck.BrandMixdeckDTO src, IUser user) {
        BrandDTO dto = new BrandDTO();
        dto.setLocalizedName(src.getLocalizedName());
        dto.setSlugName(src.getSlugName());
        dto.setCountry(src.getCountry());
        dto.setTimeZone(src.getTimeZone());
        dto.setColor(src.getColor());
        dto.setDescription(src.getDescription());
        dto.setTitleFont(src.getTitleFont());
        dto.setBitRate(src.getBitRate());
        dto.setPopularityRate(src.getPopularityRate());
        dto.setOneTimeStreamPolicy(src.getOneTimeStreamPolicy());
        dto.setSubmissionPolicy(src.getSubmissionPolicy());
        dto.setMessagingPolicy(src.getMessagingPolicy());
        dto.setIsTemporary(src.getIsTemporary());
        dto.setPublicBrand(src.getPublicBrand());
        dto.setAiOverridingEnabled(src.isAiOverridingEnabled());
        dto.setProfileOverridingEnabled(src.isProfileOverridingEnabled());
        dto.setAiOverriding(src.getAiOverriding());
        dto.setProfileOverriding(src.getProfileOverriding());
        dto.setScriptMode(src.getScriptMode());
        dto.setStreamingOptions(src.getStreamingOptions());
        dto.setCustomScript(src.getCustomScript());
        dto.setOwner(src.getOwner());
        dto.setLogoFiles(src.getLogoFiles());
        dto.setRlsActions(src.getRlsActions());
        dto.setSkipScriptValidation(src.isSkipScriptValidation());
        dto.setChatFeatureFlags(src.getChatFeatureFlags());

        Uni<Void> agentUni = (src.getAiAgentSlug() == null || src.getAiAgentSlug().isBlank())
                ? Uni.createFrom().voidItem()
                : aiAgentService.getIdBySlug(src.getAiAgentSlug(), user)
                        .invoke(dto::setAiAgentId)
                        .replaceWithVoid();
        Uni<Void> profileUni = (src.getProfileSlug() == null || src.getProfileSlug().isBlank())
                ? Uni.createFrom().voidItem()
                : profileService.getIdBySlug(src.getProfileSlug())
                        .invoke(dto::setProfileId)
                        .replaceWithVoid();
        Uni<Void> customScriptUni = (src.getCustomScriptSlug() == null || src.getCustomScriptSlug().isBlank())
                ? Uni.createFrom().voidItem()
                : scriptService.getIdBySlug(src.getCustomScriptSlug(), user)
                        .invoke(dto::setCustomScriptId)
                        .replaceWithVoid();
        Uni<Void> labelsUni = toLabelIds(src.getLabels()).invoke(dto::setLabels).replaceWithVoid();
        Uni<Void> genresUni = toLabelIds(src.getGenres()).invoke(dto::setGenres).replaceWithVoid();

        return Uni.combine().all().unis(agentUni, profileUni, customScriptUni, labelsUni, genresUni).discardItems()
                .replaceWith(dto);
    }

    private Uni<List<UUID>> toLabelIds(List<String> identifiers) {
        if (identifiers == null || identifiers.isEmpty()) {
            return Uni.createFrom().item(List.of());
        }
        List<Uni<UUID>> unis = identifiers.stream()
                .map(identifier -> labelService.findByIdentifier(identifier).map(label -> label.getId()))
                .collect(Collectors.toList());
        return Uni.join().all(unis).andFailFast();
    }

    private Uni<List<BrandScriptEntry>> resolveScriptEntries(com.semantyca.datanest.dto.brand.mixdeck.BrandMixdeckDTO dto, IUser user) {
        if (ScriptMode.CUSTOM.equals(dto.getScriptMode())) {
            return Uni.createFrom().nullItem();
        }
        if (dto.getScripts() == null || dto.getScripts().isEmpty()) {
            return Uni.createFrom().nullItem();
        }
        BrandScriptEntryMixdeckDTO first = dto.getScripts().getFirst();
        if (first.getSlugName() == null || first.getSlugName().isBlank()) {
            return Uni.createFrom().nullItem();
        }
        return scriptService.getIdBySlug(first.getSlugName(), user)
                .map(uuid -> List.of(new BrandScriptEntry(uuid, first.getUserVariables())));
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
