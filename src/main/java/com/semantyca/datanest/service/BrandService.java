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
import com.semantyca.datanest.dto.brand.AiOverridingDTO;
import com.semantyca.datanest.dto.brand.BrandDTO;
import com.semantyca.datanest.dto.brand.OwnerDTO;
import com.semantyca.datanest.dto.brand.ProfileOverridingDTO;
import com.semantyca.datanest.dto.script.BrandScriptEntryDTO;
import com.semantyca.datanest.dto.script.CustomActionDTO;
import com.semantyca.datanest.dto.script.CustomSceneDTO;
import com.semantyca.datanest.dto.script.CustomScriptDTO;
import com.semantyca.datanest.dto.script.SceneDTO;
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
import com.semantyca.mixpla.model.brand.ProfileOverriding;
import com.semantyca.mixpla.model.cnst.ManagedBy;
import com.semantyca.mixpla.model.filter.BrandFilter;
import com.semantyca.officeframe.model.cnst.CountryCode;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.net.MalformedURLException;
import java.net.URI;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

@ApplicationScoped
public class BrandService extends AbstractService<Brand, BrandDTO> {
    private static final Logger LOGGER = Logger.getLogger(BrandService.class);

    protected final ScriptService scriptService;
    protected final CommandPublisher commandPublisher;
    protected final BrandRepository repository;

    protected final SceneService sceneService;
    private final MetricPublisher metricPublisher;
    protected final DatanestConfig datanestConfig;
    protected BrandService() {
        super();
        this.scriptService = null;
        this.sceneService = null;
        this.repository = null;
        this.datanestConfig = null;
        this.metricPublisher = null;
        this.commandPublisher = null;
    }

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
                        return repository.insert(entity, rlsActions, user);
                    } else {
                        return repository.update(UUID.fromString(id), entity, rlsActions, user);
                    }
                })
                .invoke(saved -> {
                    assert commandPublisher != null;
                    commandPublisher.publishCommand(
                            CommandType.FLOW_RESTART,
                            "brand_saved",
                            Map.of("brandId", saved.getId().toString(), "slug", saved.getSlugName(), "savedBy", user.getUserName())
                    );
                })
                .chain(this::mapToDTO);
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

    public Uni<Integer> getAllOpenForSubmissionCount(IUser user) {
        assert repository != null;
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
                                assert metricPublisher != null;
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

    protected CustomSceneDTO toCustomSceneDTO(SceneDTO scene) {
        CustomSceneDTO customScene = new CustomSceneDTO();
        customScene.setName(scene.getTitle());
        if (scene.getStartTime() != null && !scene.getStartTime().isEmpty()) {
            customScene.setStartTime(scene.getStartTime().getFirst());
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

    private Uni<BrandDTO> resolveOwnerUserIds(BrandDTO dto) {
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
        doc.setIsTemporary(dto.getIsTemporary() != null ? dto.getIsTemporary() : 0);
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
        doc.setOneTimeStreamPolicy(dto.getOneTimeStreamPolicy());
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

