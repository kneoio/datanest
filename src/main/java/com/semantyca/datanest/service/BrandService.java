package com.semantyca.datanest.service;

import com.semantyca.core.dto.DocumentAccessDTO;
import com.semantyca.core.dto.rls.RlsActionDTO;
import com.semantyca.core.model.cnst.LanguageCode;
import com.semantyca.core.model.user.IUser;
import com.semantyca.core.model.user.SuperUser;
import com.semantyca.core.service.AbstractService;
import com.semantyca.core.service.UserService;
import com.semantyca.core.util.WebHelper;
import com.semantyca.datanest.config.DatanestConfig;
import com.semantyca.datanest.dto.PlaylistRequestDTO;
import com.semantyca.datanest.dto.brand.AiOverridingDTO;
import com.semantyca.datanest.dto.brand.BrandDTO;
import com.semantyca.datanest.dto.brand.mixdeck.*;
import com.semantyca.datanest.dto.brand.OwnerDTO;
import com.semantyca.datanest.dto.brand.ProfileOverridingDTO;
import com.semantyca.datanest.dto.sharing.ShareTargetBrandDTO;
import com.semantyca.datanest.dto.script.BrandScriptEntryDTO;
import com.semantyca.datanest.dto.script.CustomActionDTO;
import com.semantyca.datanest.dto.script.CustomSceneDTO;
import com.semantyca.datanest.dto.script.CustomScriptDTO;
import com.semantyca.datanest.dto.script.AbsoluteSceneDTO;
import com.semantyca.datanest.dto.script.AbstractSceneDTO;
import com.semantyca.datanest.messaging.CommandPublisher;
import com.semantyca.datanest.messaging.MetricPublisher;
import com.semantyca.datanest.model.cnst.ScriptMode;
import com.semantyca.datanest.repository.BrandRepository;
import com.semantyca.mixpla.dto.queue.command.CommandType;
import com.semantyca.mixpla.dto.queue.metric.MetricEventType;
import com.semantyca.mixpla.dto.queue.metric.ProcessType;
import com.semantyca.mixpla.model.brand.AiOverriding;
import com.semantyca.mixpla.model.brand.Brand;
import com.semantyca.mixpla.model.brand.BrandScriptEntry;
import com.semantyca.mixpla.model.brand.Owner;
import com.semantyca.mixpla.model.brand.StreamHistoryEntry;
import com.semantyca.mixpla.model.brand.ProfileOverriding;
import com.semantyca.mixpla.model.cnst.ManagedBy;
import com.semantyca.mixpla.model.cnst.SubmissionPolicy;
import com.semantyca.mixpla.model.filter.BrandFilter;
import com.semantyca.mixpla.repository.UserSubscriptionRepository;
import com.semantyca.officeframe.model.cnst.CountryCode;
import com.semantyca.datanest.service.soundfragment.SoundFragmentService;
import com.semantyca.officeframe.service.GenreService;
import com.semantyca.officeframe.service.LabelService;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.net.MalformedURLException;
import java.net.URI;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

@ApplicationScoped
public class BrandService extends AbstractService<Brand, BrandDTO> {
    private static final Logger LOGGER = Logger.getLogger(BrandService.class);

    protected final ScriptService scriptService;
    protected final AiAgentService aiAgentService;
    protected final ProfileService profileService;
    protected final LabelService labelService;
    protected final GenreService genreService;
    protected final PromptService promptService;
    protected final CommandPublisher commandPublisher;
    protected final BrandRepository repository;

    protected final SceneService sceneService;
    protected final MetricPublisher metricPublisher;
    protected final DatanestConfig datanestConfig;
    private final UserSubscriptionRepository userSubscriptionRepository;
    // Lazy: SoundFragmentService depends on BrandService, so a direct injection would be circular.
    protected final Instance<SoundFragmentService> soundFragmentServiceSource;

    protected BrandService() {
        super();
        this.scriptService = null;
        this.aiAgentService = null;
        this.profileService = null;
        this.labelService = null;
        this.genreService = null;
        this.promptService = null;
        this.sceneService = null;
        this.repository = null;
        this.datanestConfig = null;
        this.metricPublisher = null;
        this.commandPublisher = null;
        this.userSubscriptionRepository = null;
        this.soundFragmentServiceSource = null;
    }

    @Inject
    public BrandService(
            UserService userService,
            ScriptService scriptService,
            AiAgentService aiAgentService,
            ProfileService profileService,
            LabelService labelService,
            GenreService genreService,
            PromptService promptService,
            SceneService sceneService,
            BrandRepository repository,
            DatanestConfig datanestConfig,
            MetricPublisher metricPublisher,
            CommandPublisher commandPublisher,
            UserSubscriptionRepository userSubscriptionRepository,
            Instance<SoundFragmentService> soundFragmentServiceSource
    ) {
        super(userService);
        this.scriptService = scriptService;
        this.aiAgentService = aiAgentService;
        this.profileService = profileService;
        this.labelService = labelService;
        this.genreService = genreService;
        this.promptService = promptService;
        this.sceneService = sceneService;
        this.repository = repository;
        this.datanestConfig = datanestConfig;
        this.metricPublisher = metricPublisher;
        this.commandPublisher = commandPublisher;
        this.userSubscriptionRepository = userSubscriptionRepository;
        this.soundFragmentServiceSource = soundFragmentServiceSource;
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

    public Uni<List<BrandPublicFlatDTO>> getAllPublicFlatDTO(final IUser user, final BrandFilter filter) {
        assert repository != null;
        return repository.getAll(100, 0, false, user, filter)
                .map(list -> list.stream().map(this::toPublicFlatDTO).collect(Collectors.toList()));
    }

    public Uni<BrandPublicFlatDTO> getPublicFlatDTOBySlug(final String slugName, final IUser user) {
        assert repository != null;
        return repository.getBySlugName(slugName, user, false).map(this::toPublicFlatDTO);
    }

    public Uni<BrandMixdeckDTO> getDTOBySlug(final String slugName, final IUser user, final LanguageCode language) {
        assert repository != null;
        return repository.getBySlugName(slugName, user, false).chain(this::mapToMixdeckDTO);
    }

    /**
     * Mixdeck form mapping — separate DTO; script entries use slugName (no brand UUID).
     */
    Uni<BrandMixdeckDTO> mapToMixdeckDTO(Brand doc) {
        assert repository != null;
        return Uni.combine().all().unis(
                userService.getUserName(doc.getAuthor()),
                userService.getUserName(doc.getLastModifier()),
                repository.getScriptEntryDTOsForBrand(doc.getId())
        ).asTuple().chain(tuple -> {
            BrandMixdeckDTO dto = new BrandMixdeckDTO();
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
            dto.setOneTimeStreamPolicy(doc.getOneTimeStreamPolicy());
            dto.setSubmissionPolicy(doc.getSubmissionPolicy());
            dto.setMessagingPolicy(doc.getMessagingPolicy());
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
                assert datanestConfig != null;
                dto.setHlsUrl(URI.create(datanestConfig.getHost() + "/live/" + dto.getSlugName() + "/opus").toURL());
                dto.setMp3Url(URI.create(datanestConfig.getHost() + "/live/" + dto.getSlugName() + "/mp3").toURL());
                dto.setMixplaUrl(URI.create("https://mixpla.online/" + dto.getSlugName()).toURL());
            } catch (MalformedURLException e) {
                throw new RuntimeException(e);
            }

            ScriptMode earlyMode = ScriptMode.valueOf(doc.getScriptMode() != null ? doc.getScriptMode() : ScriptMode.PREDEFINED.name());
            if (!ScriptMode.CUSTOM.equals(earlyMode)) {
                dto.setScripts(tuple.getItem3());
            }

            if (doc.getOwner() != null) {
                OwnerMixdeckDTO ownerDTO = new OwnerMixdeckDTO();
                ownerDTO.setName(doc.getOwner().getName());
                ownerDTO.setEmail(doc.getOwner().getEmail());
                ownerDTO.setExposeWhileSharing(doc.getOwner().isExposeWhileSharing());
                ownerDTO.setActionDebugEnabled(doc.getOwner().isActionDebugEnabled());
                if (doc.getOwner().getCoOwners() != null) {
                    ownerDTO.setCoOwners(doc.getOwner().getCoOwners().stream()
                            .map(co -> {
                                OwnerMixdeckDTO coDTO = new OwnerMixdeckDTO();
                                coDTO.setName(co.getName());
                                coDTO.setEmail(co.getEmail());
                                coDTO.setExposeWhileSharing(co.isExposeWhileSharing());
                                coDTO.setActionDebugEnabled(co.isActionDebugEnabled());
                                return coDTO;
                            })
                            .collect(Collectors.toList()));
                }
                dto.setOwner(ownerDTO);
            }
            dto.setLogoFiles(doc.getFileMetadataList().isEmpty()
                    ? null
                    : doc.getFileMetadataList().stream().map(FileMixdeckDTO::from).collect(Collectors.toList()));
            dto.setScriptMode(earlyMode);
            dto.setStreamingOptions(doc.getStreamingOptions());
            dto.setChatFeatureFlags(doc.getChatFeatureFlags());

            List<StreamHistoryEntry> streamHistory = doc.getStreamHistory();
            if (streamHistory != null && !streamHistory.isEmpty()) {
                dto.setLastStreamHistoryEntry(streamHistory.get(streamHistory.size() - 1));
            }

            Uni<Void> agentUni = doc.getAiAgentId() == null
                    ? Uni.createFrom().voidItem()
                    : aiAgentService.getSlugById(doc.getAiAgentId())
                            .invoke(dto::setAiAgentSlug)
                            .replaceWithVoid();
            Uni<Void> profileUni = doc.getProfileId() == null
                    ? Uni.createFrom().voidItem()
                    : profileService.getSlugById(doc.getProfileId())
                            .invoke(dto::setProfileSlug)
                            .replaceWithVoid();
            Uni<Void> customScriptUni = (!ScriptMode.CUSTOM.equals(earlyMode) || doc.getCustomScriptId() == null)
                    ? Uni.createFrom().voidItem()
                    : scriptService.getSlugById(doc.getCustomScriptId())
                            .invoke(dto::setCustomScriptSlug)
                            .replaceWithVoid();
            Uni<Void> labelsUni = toLabelIdentifiers(doc.getLabels()).invoke(dto::setLabels).replaceWithVoid();
            Uni<Void> genresUni = toGenreIdentifiers(doc.getGenres()).invoke(dto::setGenres).replaceWithVoid();

            return Uni.combine().all().unis(agentUni, profileUni, customScriptUni, labelsUni, genresUni).discardItems()
                    .chain(v -> {
                        if (ScriptMode.CUSTOM.equals(earlyMode) && doc.getCustomScriptId() != null) {
                            assert scriptService != null;
                            return scriptService.getById(doc.getCustomScriptId(), SuperUser.build())
                                    .chain(customScript -> {
                                        assert sceneService != null;
                                        return sceneService.getAllByScript(customScript.getId(), 1000, 0, SuperUser.build())
                                                .chain(sceneDTOs -> {
                                                    CustomScriptMixdeckDTO customScriptDTO = new CustomScriptMixdeckDTO();
                                                    customScriptDTO.setTitle(customScript.getName());
                                                    customScriptDTO.setColor(customScript.getColor());
                                                    if (sceneDTOs.isEmpty()) {
                                                        customScriptDTO.setScenes(List.of());
                                                        dto.setCustomScript(customScriptDTO);
                                                        return Uni.createFrom().item(dto);
                                                    }
                                                    List<Uni<CustomSceneMixdeckDTO>> sceneUnis = sceneDTOs.stream()
                                                            .map(this::toCustomSceneMixdeckDTO)
                                                            .collect(Collectors.toList());
                                                    return Uni.join().all(sceneUnis).andFailFast()
                                                            .map(scenes -> {
                                                                customScriptDTO.setScenes(scenes);
                                                                dto.setCustomScript(customScriptDTO);
                                                                return dto;
                                                            });
                                                });
                                    });
                        }
                        return Uni.createFrom().item(dto);
                    });
        });
    }

    private Uni<CustomSceneMixdeckDTO> toCustomSceneMixdeckDTO(AbstractSceneDTO scene) {
        CustomSceneMixdeckDTO customScene = new CustomSceneMixdeckDTO();
        customScene.setName(scene.getTitle());
        if (scene instanceof AbsoluteSceneDTO absoluteScene
                && absoluteScene.getStartTime() != null && !absoluteScene.getStartTime().isEmpty()) {
            customScene.setStartTime(absoluteScene.getStartTime().getFirst());
        }
        customScene.setTalkativity(scene.getTalkativity());
        customScene.setAllowJingles(scene.isAllowJingles());
        customScene.setAllowAds(scene.isAllowAds());

        List<CustomActionMixdeckDTO> merged = new ArrayList<>();
        List<Uni<Void>> resolutions = new ArrayList<>();
        if (scene.getActions() != null) {
            scene.getActions().forEach(a -> {
                CustomActionMixdeckDTO action = new CustomActionMixdeckDTO();
                action.setType("custom");
                action.setName(a.getName());
                action.setInstruction(a.getInstruction());
                action.setContextVars(a.getContextVars());
                merged.add(action);
            });
        }
        if (scene.getPrompts() != null) {
            scene.getPrompts().forEach(p -> {
                CustomActionMixdeckDTO action = new CustomActionMixdeckDTO();
                action.setType("predefined");
                merged.add(action);
                if (p.getPromptId() != null) {
                    resolutions.add(promptService.getSlugById(p.getPromptId())
                            .invoke(action::setActionSlug)
                            .replaceWithVoid());
                }
            });
        }
        if (!merged.isEmpty()) {
            customScene.setActions(merged);
        }

        Uni<Void> playlistUni = toPlaylistRequestMixdeckDTO(scene.getPlaylistRequest())
                .invoke(customScene::setStagePlaylist)
                .replaceWithVoid();

        List<Uni<Void>> all = new ArrayList<>(resolutions);
        all.add(playlistUni);
        return Uni.join().all(all).andFailFast().replaceWith(customScene);
    }

    private Uni<PlaylistRequestMixdeckDTO> toPlaylistRequestMixdeckDTO(PlaylistRequestDTO src) {
        if (src == null) {
            return Uni.createFrom().nullItem();
        }
        PlaylistRequestMixdeckDTO dto = new PlaylistRequestMixdeckDTO();
        dto.setMixingType(src.getMixingType());
        dto.setMixingArtefacts(src.getMixingArtefacts());
        dto.setSourcing(src.getSourcing());
        dto.setTitle(src.getTitle());
        dto.setArtist(src.getArtist());
        dto.setType(src.getType());
        dto.setSource(src.getSource());
        dto.setSearchTerm(src.getSearchTerm());

        List<Uni<Void>> resolutions = new ArrayList<>();
        resolutions.add(toGenreIdentifiers(src.getGenres()).invoke(dto::setGenres).replaceWithVoid());
        resolutions.add(toLabelIdentifiers(src.getLabels()).invoke(dto::setLabels).replaceWithVoid());
        resolutions.add(toSoundFragmentSlugs(src.getSoundFragments()).invoke(dto::setSoundFragments).replaceWithVoid());
        if (src.getPrompts() != null && !src.getPrompts().isEmpty()) {
            List<ScenePromptMixdeckDTO> prompts = new ArrayList<>();
            src.getPrompts().forEach(p -> {
                ScenePromptMixdeckDTO sp = new ScenePromptMixdeckDTO();
                sp.setActive(p.isActive());
                sp.setMandatory(p.isMandatory());
                sp.setRank(p.getRank());
                sp.setWeight(p.getWeight());
                prompts.add(sp);
                if (p.getPromptId() != null) {
                    resolutions.add(promptService.getSlugById(p.getPromptId())
                            .invoke(sp::setPromptSlug)
                            .replaceWithVoid());
                }
            });
            dto.setPrompts(prompts);
        }
        return Uni.join().all(resolutions).andFailFast().replaceWith(dto);
    }

    private Uni<List<String>> toSoundFragmentSlugs(List<UUID> ids) {
        if (ids == null || ids.isEmpty()) {
            return Uni.createFrom().item(List.of());
        }
        SoundFragmentService soundFragmentService = soundFragmentServiceSource.get();
        List<Uni<String>> unis = ids.stream()
                .map(soundFragmentService::getSlugById)
                .collect(Collectors.toList());
        return Uni.join().all(unis).andFailFast();
    }

    private Uni<List<String>> toLabelIdentifiers(List<UUID> ids) {
        if (ids == null || ids.isEmpty()) {
            return Uni.createFrom().item(List.of());
        }
        List<Uni<String>> unis = ids.stream()
                .map(id -> labelService.getById(id).map(label -> label.getIdentifier()))
                .collect(Collectors.toList());
        return Uni.join().all(unis).andFailFast();
    }

    private Uni<List<String>> toGenreIdentifiers(List<UUID> ids) {
        if (ids == null || ids.isEmpty()) {
            return Uni.createFrom().item(List.of());
        }
        List<Uni<String>> unis = ids.stream()
                .map(id -> genreService.getById(id).map(genre -> genre.getIdentifier()))
                .collect(Collectors.toList());
        return Uni.join().all(unis).andFailFast();
    }

    private BrandPublicFlatDTO toPublicFlatDTO(Brand doc) {
        BrandPublicFlatDTO dto = new BrandPublicFlatDTO();
        dto.setRegDate(doc.getRegDate());
        dto.setLastModifiedDate(doc.getLastModifiedDate());
        dto.setLocalizedName(doc.getLocalizedName());
        dto.setSlugName(doc.getSlugName());
        dto.setCountry(doc.getCountry() != null ? doc.getCountry().name() : null);
        dto.setColor(doc.getColor());
        dto.setDescription(doc.getDescription());
        dto.setTitleFont(doc.getTitleFont());
        dto.setBitRate(doc.getBitRate());
        dto.setPopularityRate(doc.getPopularityRate());
        if (doc.getTimeZone() != null) {
            dto.setTimeZone(doc.getTimeZone().getId());
        }
        dto.setPublicBrand(doc.getPublicBrand());
        if (doc.getOwner() != null) {
            dto.setOwner(doc.getOwner().getName());
            dto.setOwnerEmail(doc.getOwner().getEmail());
        }
        dto.setOneTimeStreamPolicy(doc.getOneTimeStreamPolicy());
        dto.setSubmissionPolicy(doc.getSubmissionPolicy());
        dto.setMessagingPolicy(doc.getMessagingPolicy());
        try {
            assert datanestConfig != null;
            dto.setHlsUrl(URI.create(datanestConfig.getHost() + "/live/" + doc.getSlugName() + "/opus").toURL());
            dto.setMp3Url(URI.create(datanestConfig.getHost() + "/live/" + doc.getSlugName() + "/mp3").toURL());
            dto.setMixplaUrl(URI.create("https://mixpla.online/" + doc.getSlugName()).toURL());
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
        return dto;
    }

    public Uni<List<Brand>> getAll(final int limit, final int offset) {
        assert repository != null;
        return repository.getAll(limit, offset, false, SuperUser.build(), null);
    }

    public Uni<List<Brand>> getAll(final int limit, final int offset, IUser user) {
        assert repository != null;
        return repository.getAll(limit, offset, false, user, null);
    }

    public Uni<Brand> getById(UUID id, IUser user) {
        assert repository != null;
        return repository.findById(id, user, true);
    }

    public Uni<Brand> getBySlugNameForUser(String name, IUser user) {
        assert repository != null;
        return repository.getBySlugName(name, user, false);
    }

    public Uni<Brand> getBySlugName(String name) {
        assert repository != null;
        return repository.getBySlugName(name)
                .chain(brand -> {
                    if (brand == null) {
                        return Uni.createFrom().nullItem();
                    }
                    assert scriptService != null;
                    return scriptService.getAllScriptsForBrandWithScenes(brand.getId(), SuperUser.build())
                            .map(brandScripts -> {
                                List<BrandScriptEntry> entries = brandScripts.stream()
                                        .map(bs -> new BrandScriptEntry(
                                                bs.getScript().getId(),
                                                bs.getUserVariables()
                                        ))
                                        .collect(Collectors.toList());
                                brand.setScriptIds(entries);
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
        List<RlsActionDTO> rlsActions = dto.getRlsActions() != null ? dto.getRlsActions() : List.of();

        boolean isNew = "new".equalsIgnoreCase(id) || id == null || id.isBlank();
        Uni<Brand> existingUni = isNew
                ? Uni.createFrom().nullItem()
                : repository.findById(UUID.fromString(id), user, false);

        return existingUni
                .chain(existing -> resolveOwnerUserIds(dto).map(resolvedDto -> buildEntity(
                        resolvedDto, user,
                        existing != null ? existing.getSlugName() : WebHelper.generateSlug(dto.getLocalizedName()),
                        existing != null ? existing.getOwner() : null)))
                .chain(entity -> {
                    if (isNew) {
                        entity.setPopularityRate(5);
                        return checkSubscriptionStationLimit(user)
                                .chain(() -> repository.insert(entity, rlsActions, user));
                    } else {
                        return repository.update(UUID.fromString(id), entity, rlsActions, user);
                    }
                })
                .invoke(saved -> {
                    assert commandPublisher != null;
                    UUID traceId = UUID.randomUUID();
                    LOGGER.infof("BrandService.upsert: brand saved id=%s slug=%s traceId=%s",
                            saved.getId(), saved.getSlugName(), traceId);
                    commandPublisher.publishCommand(
                            CommandType.FLOW_RESTART,
                            "brand_saved",
                            Map.of("brandId", saved.getId().toString(), "slug", saved.getSlugName(), "savedBy", user.getUserName()),
                            traceId
                    );
                    assert metricPublisher != null;
                    metricPublisher.publishMetric(
                            saved.getSlugName(),
                            MetricEventType.COMMAND,
                            ProcessType.FLOW,
                            "brand_saved",
                            Map.of(
                                    "brandId", saved.getId().toString(),
                                    "slug", saved.getSlugName(),
                                    "savedBy", user.getUserName(),
                                    "commandType", CommandType.FLOW_RESTART.name()
                            ),
                            traceId
                    );
                })
                .chain(this::mapToDTO);
    }



    private Uni<Void> checkSubscriptionStationLimit(IUser user) {
        assert userSubscriptionRepository != null;
        assert repository != null;
        return userSubscriptionRepository.findActiveByUserId(user.getId())
                .onItem().transformToUni(subscription -> {
                    if (subscription == null || subscription.getMaxStations() == null) {
                        return Uni.createFrom().failure(new IllegalStateException(
                                "Station limit reached: no active subscription found"));
                    }
                    return repository.getAllCount(user, false, null)
                            .chain(count -> count >= subscription.getMaxStations()
                                    ? Uni.createFrom().failure(new IllegalStateException(
                                            "Station limit reached: your subscription allows " + subscription.getMaxStations() + " stations"))
                                    : Uni.createFrom().voidItem());
                });
    }

    public Uni<List<BrandDTO>> getAllOpenForSubmissionDTO(int limit, int offset, IUser user) {
        assert repository != null;
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

    public Uni<List<ShareTargetBrandDTO>> getAllOpenForSubmissionShareTargets(int limit, int offset, IUser user) {
        assert repository != null;
        return repository.getAllOpenForSubmission(limit, offset, user.getId())
                .chain(list -> {
                    if (list.isEmpty()) {
                        return Uni.createFrom().item(List.of());
                    }
                    List<Uni<ShareTargetBrandDTO>> unis = list.stream()
                            .map(this::toShareTargetBrandDTO)
                            .collect(Collectors.toList());
                    return Uni.join().all(unis).andFailFast();
                });
    }

    private Uni<ShareTargetBrandDTO> toShareTargetBrandDTO(Brand doc) {
        return toGenreIdentifiers(doc.getGenres()).map(genres -> {
            ShareTargetBrandDTO dto = new ShareTargetBrandDTO();
            dto.setSlugName(doc.getSlugName());
            if (doc.getLocalizedName() != null && doc.getLocalizedName().containsKey(LanguageCode.en)) {
                EnumMap<LanguageCode, String> localizedName = new EnumMap<>(LanguageCode.class);
                localizedName.put(LanguageCode.en, doc.getLocalizedName().get(LanguageCode.en));
                dto.setLocalizedName(localizedName);
            }
            dto.setGenres(genres);
            return dto;
        });
    }

    public Uni<Integer> getAllOpenForSubmissionCount(IUser user) {
        assert repository != null;
        return repository.getAllOpenForSubmissionCount(user.getId());
    }

    public Uni<Integer> archive(String id, IUser user) {
        assert repository != null;
        UUID brandId = UUID.fromString(id);
        return repository.findById(brandId, user, false)
                .chain(brand -> repository.archive(brandId, user)
                        .invoke(count -> {
                            if (count > 0 && brand.getSlugName() != null) {
                                assert commandPublisher != null;
                                assert metricPublisher != null;
                                UUID traceId = UUID.randomUUID();
                                commandPublisher.publishCommand(
                                        CommandType.AIVOX_STOP_BRAND,
                                        "brand_deleted",
                                        Map.of("brandId", brand.getId().toString(), "slug", brand.getSlugName()),
                                        traceId
                                );
                                metricPublisher.publishMetric(
                                        brand.getSlugName(),
                                        MetricEventType.COMMAND,
                                        ProcessType.FLOW,
                                        "brand_deleted",
                                        Map.of(
                                                "brandId", brand.getId().toString(),
                                                "slug", brand.getSlugName(),
                                                "deletedBy", user.getUserName(),
                                                "commandType", CommandType.AIVOX_STOP_BRAND.name()
                                        ),
                                        traceId
                                );
                            }
                        }));
    }

    public Uni<Integer> archive(UUID id) {
        assert repository != null;
        return repository.archive(id, SuperUser.build());
    }

    public Uni<Integer> closeBrand(String slugName, IUser user) {
        assert repository != null;
        return repository.getBySlugName(slugName, user, false)
                .chain(brand -> {
                    UUID brandId = brand.getId();
                    return repository.closeBrand(brandId, user)
                            .invoke(count -> {
                                if (count > 0) {
                                    assert metricPublisher != null;
                                    metricPublisher.publishMetric(
                                            brand.getSlugName(),
                                            MetricEventType.WARNING,
                                            ProcessType.INDEPENDENT,
                                            "brand_closed",
                                            Map.of("brandId", brandId.toString(), "closedBy", user.getUserName())
                                    );
                                }
                            });
                });
    }

    Uni<BrandDTO> mapToDTO(Brand doc) {
        assert repository != null;
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
                //https://mixpla.online/live/aye-aye-s-ear/master.m3u8
                assert datanestConfig != null;
                https://mixpla.online/aivox/aye-aye-s-ear/master.m3u8
                //dto.setHlsUrl(URI.create(datanestConfig.getHost() + "/live/" + dto.getSlugName() + "/master.m3u8").toURL());
                dto.setHlsUrl(URI.create(datanestConfig.getHost() + "/live/" + dto.getSlugName() + "/opus").toURL());
                //dto.setIceCastUrl(URI.create(datanestConfig.getHost() + "/" + dto.getSlugName() + "/radio/icecast").toURL());
                dto.setMp3Url(URI.create(datanestConfig.getHost() + "/live/" + dto.getSlugName() + "/mp3").toURL());
                dto.setMixplaUrl(URI.create("https://mixpla.online/" + dto.getSlugName()).toURL());
            } catch (MalformedURLException e) {
                throw new RuntimeException(e);
            }
            ScriptMode earlyMode = ScriptMode.valueOf(doc.getScriptMode() != null ? doc.getScriptMode() : ScriptMode.PREDEFINED.name());
            if (!ScriptMode.CUSTOM.equals(earlyMode)) {
                List<BrandScriptEntryDTO> scriptDTOs = tuple.getItem3().stream()
                        .map(entry -> {
                            BrandScriptEntryDTO scriptDTO = new BrandScriptEntryDTO();
                            scriptDTO.setScriptId(entry.getScriptId());
                            scriptDTO.setUserVariables(entry.getUserVariables());
                            return scriptDTO;
                        })
                        .collect(Collectors.toList());
                dto.setScriptIds(scriptDTOs);
            } else {
                dto.setCustomScriptId(doc.getCustomScriptId());
            }
            if (doc.getOwner() != null) {
                OwnerDTO ownerDTO = new OwnerDTO();
                ownerDTO.setUserId(doc.getOwner().getUserId());
                ownerDTO.setName(doc.getOwner().getName());
                ownerDTO.setEmail(doc.getOwner().getEmail());
                ownerDTO.setExposeWhileSharing(doc.getOwner().isExposeWhileSharing());
                ownerDTO.setActionDebugEnabled(doc.getOwner().isActionDebugEnabled());
                if (doc.getOwner().getCoOwners() != null) {
                    ownerDTO.setCoOwners(doc.getOwner().getCoOwners().stream()
                            .map(co -> {
                                OwnerDTO coDTO = new OwnerDTO();
                                coDTO.setUserId(co.getUserId());
                                coDTO.setName(co.getName());
                                coDTO.setEmail(co.getEmail());
                                coDTO.setExposeWhileSharing(co.isExposeWhileSharing());
                                coDTO.setActionDebugEnabled(co.isActionDebugEnabled());
                                return coDTO;
                            })
                            .collect(Collectors.toList()));
                }
                dto.setOwner(ownerDTO);
            }
            dto.setLabels(doc.getLabels());
            dto.setGenres(doc.getGenres());
            dto.setLogoFiles(doc.getFileMetadataList().isEmpty() ? null : doc.getFileMetadataList());

            dto.setScriptMode(earlyMode);
            dto.setStreamingOptions(doc.getStreamingOptions());
            dto.setChatFeatureFlags(doc.getChatFeatureFlags());

            List<StreamHistoryEntry> streamHistory = doc.getStreamHistory();
            if (streamHistory != null && !streamHistory.isEmpty()) {
                dto.setLastStreamHistoryEntry(streamHistory.get(streamHistory.size() - 1));
            }

            if (ScriptMode.CUSTOM.equals(earlyMode) && doc.getCustomScriptId() != null) {
                assert scriptService != null;
                return scriptService.getById(doc.getCustomScriptId(), SuperUser.build())
                        .chain(customScript -> {
                            assert sceneService != null;
                            return sceneService.getAllByScript(customScript.getId(), 1000, 0, SuperUser.build())
                                    .map(sceneDTOs -> {
                                        CustomScriptDTO customScriptDTO = new CustomScriptDTO();
                                        customScriptDTO.setTitle(customScript.getName());
                                        customScriptDTO.setColor(customScript.getColor());
                                        customScriptDTO.setScenes(sceneDTOs.stream()
                                                .map(this::toCustomSceneDTO)
                                                .collect(Collectors.toList()));
                                        dto.setCustomScript(customScriptDTO);
                                        return dto;
                                    });
                        });
            }
            return Uni.createFrom().item(dto);
        });
    }

    protected CustomSceneDTO toCustomSceneDTO(AbstractSceneDTO scene) {
        CustomSceneDTO customScene = new CustomSceneDTO();
        customScene.setName(scene.getTitle());
        if (scene instanceof AbsoluteSceneDTO absoluteScene
                && absoluteScene.getStartTime() != null && !absoluteScene.getStartTime().isEmpty()) {
            customScene.setStartTime(absoluteScene.getStartTime().getFirst());
        }
        customScene.setTalkativity(scene.getTalkativity());
        customScene.setAllowJingles(scene.isAllowJingles());
        customScene.setAllowAds(scene.isAllowAds());
        customScene.setStagePlaylist(scene.getPlaylistRequest());
        List<CustomActionDTO> merged = new ArrayList<>();
        if (scene.getActions() != null) {
            scene.getActions().forEach(a -> a.setType("custom"));
            merged.addAll(scene.getActions());
        }
        if (scene.getPrompts() != null) {
            scene.getPrompts().forEach(p -> {
                CustomActionDTO a = new CustomActionDTO();
                a.setType("predefined");
                a.setActionId(p.getPromptId());
                merged.add(a);
            });
        }
        if (!merged.isEmpty()) customScene.setActions(merged);
        return customScene;
    }

    protected Uni<BrandDTO> resolveOwnerUserIds(BrandDTO dto) {
        if (dto.getOwner() == null) return Uni.createFrom().item(dto);

        List<Uni<Void>> resolutions = new ArrayList<>();
        OwnerDTO owner = dto.getOwner();

        if (owner.getEmail() != null) {
            resolutions.add(userService.findByEmail(owner.getEmail())
                    .onFailure().recoverWithNull()
                    .invoke(u -> {
                        if (u != null) {
                            owner.setUserId(u.getId());
                            owner.setName(u.getUserName());
                        }
                    })
                    .replaceWithVoid());
        }

        if (owner.getCoOwners() != null) {
            for (OwnerDTO co : owner.getCoOwners()) {
                if (co.getEmail() != null) {
                    resolutions.add(userService.findByEmail(co.getEmail())
                            .onFailure().recoverWithNull()
                            .invoke(u -> {
                                if (u != null) {
                                    co.setUserId(u.getId());
                                    co.setName(u.getUserName());
                                }
                            })
                            .replaceWithVoid());
                }
            }
        }

        if (resolutions.isEmpty()) return Uni.createFrom().item(dto);
        return Uni.combine().all().unis(resolutions).discardItems().replaceWith(dto);
    }

    Brand buildEntity(BrandDTO dto, IUser user, String slug) {
        return buildEntity(dto, user, slug, null);
    }

    Brand buildEntity(BrandDTO dto, IUser user, String slug, Owner existingOwner) {
        Brand doc = new Brand();
        doc.setLocalizedName(dto.getLocalizedName());
        doc.setCountry(CountryCode.fromString(dto.getCountry()));
        doc.setPublicBrand(dto.getPublicBrand());
        doc.setManagedBy(ManagedBy.MIX);
        doc.setColor(dto.getColor());
        doc.setDescription(dto.getDescription());
        doc.setTitleFont(dto.getTitleFont());
        doc.setTimeZone(ZoneId.of(dto.getTimeZone()));
        doc.setSlugName(slug);
        doc.setBitRate(dto.getBitRate());
        doc.setAiAgentId(dto.getAiAgentId());
        doc.setProfileId(dto.getProfileId());
        doc.setOneTimeStreamPolicy(SubmissionPolicy.NO_RESTRICTIONS);
        doc.setSubmissionPolicy(dto.getSubmissionPolicy());
        doc.setMessagingPolicy(dto.getMessagingPolicy());

        if (dto.getAiOverriding() != null) {
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

        if (dto.getOwner() != null) {
            Owner owner = new Owner();
            Long incomingOwnerId = dto.getOwner().getUserId();
            if (incomingOwnerId != null && incomingOwnerId > 0) {
                owner.setUserId(incomingOwnerId);
            } else if (existingOwner != null && existingOwner.getUserId() != null) {
                owner.setUserId(existingOwner.getUserId());
            } else {
                owner.setUserId(user.getId());
            }
            owner.setName(dto.getOwner().getName());
            owner.setEmail(dto.getOwner().getEmail());
            owner.setExposeWhileSharing(dto.getOwner().isExposeWhileSharing());
            owner.setActionDebugEnabled(dto.getOwner().isActionDebugEnabled());
            if (dto.getOwner().getCoOwners() != null) {
                owner.setCoOwners(dto.getOwner().getCoOwners().stream()
                        .map(co -> {
                            Owner coOwner = new Owner();
                            coOwner.setUserId(co.getUserId());
                            coOwner.setName(co.getName());
                            coOwner.setEmail(co.getEmail());
                            coOwner.setExposeWhileSharing(co.isExposeWhileSharing());
                            coOwner.setActionDebugEnabled(co.isActionDebugEnabled());
                            return coOwner;
                        })
                        .collect(Collectors.toList()));
            } else if (existingOwner != null) {
                owner.setCoOwners(existingOwner.getCoOwners());
            }
            doc.setOwner(owner);
        } else if (existingOwner != null) {
            doc.setOwner(existingOwner);
        }
     /*   LOGGER.infof("buildEntity owner resolution: slug=%s incomingOwnerId=%s incomingCoOwners=%s existingOwnerId=%s existingCoOwners=%s -> resultOwnerId=%s resultCoOwners=%s",
                slug,
                dto.getOwner() != null ? dto.getOwner().getUserId() : null,
                dto.getOwner() != null && dto.getOwner().getCoOwners() != null ? dto.getOwner().getCoOwners().stream().map(OwnerDTO::getUserId).toList() : null,
                existingOwner != null ? existingOwner.getUserId() : null,
                existingOwner != null && existingOwner.getCoOwners() != null ? existingOwner.getCoOwners().stream().map(Owner::getUserId).toList() : null,
                doc.getOwner() != null ? doc.getOwner().getUserId() : null,
                doc.getOwner() != null && doc.getOwner().getCoOwners() != null ? doc.getOwner().getCoOwners().stream().map(Owner::getUserId).toList() : null);*/

        if (dto.getLabels() != null) {
            doc.setLabels(dto.getLabels());
        }

        if (dto.getGenres() != null) {
            doc.setGenres(dto.getGenres());
        }

        if (ScriptMode.CUSTOM.equals(dto.getScriptMode())) {
            doc.setCustomScriptId(dto.getCustomScriptId());
        } else if (dto.getScriptIds() != null && !dto.getScriptIds().isEmpty()) {
            BrandScriptEntryDTO first = dto.getScriptIds().getFirst();
            doc.setScriptIds(List.of(new BrandScriptEntry(first.getScriptId(), first.getUserVariables())));
        }
        doc.setScriptMode(dto.getScriptMode() != null ? dto.getScriptMode().name() : ScriptMode.PREDEFINED.name());
        doc.setStreamingOptions(dto.getStreamingOptions());
        doc.setChatFeatureFlags(dto.getChatFeatureFlags());

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

