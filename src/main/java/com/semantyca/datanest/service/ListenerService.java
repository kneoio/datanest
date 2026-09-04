package com.semantyca.datanest.service;

import com.semantyca.core.dto.DocumentAccessDTO;
import com.semantyca.core.dto.document.UserDTO;
import com.semantyca.core.dto.rls.RlsActionDTO;
import com.semantyca.core.model.UserData;
import com.semantyca.core.model.cnst.LanguageCode;
import com.semantyca.core.model.user.IUser;
import com.semantyca.core.model.user.UndefinedUser;
import com.semantyca.core.service.AbstractService;
import com.semantyca.core.service.UserService;
import com.semantyca.core.util.WebHelper;
import com.semantyca.datanest.dto.BrandListenerDTO;
import com.semantyca.datanest.dto.ListenerDTO;
import com.semantyca.datanest.dto.brand.mixdeck.BrandListenerMixdeckDTO;
import com.semantyca.datanest.dto.brand.mixdeck.ListenerMixdeckDTO;
import com.semantyca.datanest.repository.ListenersRepository;
import com.semantyca.datanest.util.DocumentIds;
import com.semantyca.mixpla.model.BrandListener;
import com.semantyca.mixpla.model.Listener;
import com.semantyca.mixpla.model.brand.Brand;
import com.semantyca.mixpla.model.filter.ListenerFilter;
import com.semantyca.officeframe.model.Label;
import com.semantyca.officeframe.service.LabelService;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.validation.Validator;
import org.jboss.logging.Logger;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

@ApplicationScoped
public class ListenerService extends AbstractService<Listener, ListenerDTO> {
    private static final Logger LOGGER = Logger.getLogger(ListenerService.class);
    private final ListenersRepository repository;
    private final Validator validator;
    private BrandService brandService;
    private final LabelService labelService;

    protected ListenerService() {
        super();
        this.repository = null;
        this.validator = null;
        this.labelService = null;
    }

    @Inject
    public ListenerService(UserService userService,
                           BrandService brandService,
                           LabelService labelService,
                           Validator validator,
                           ListenersRepository repository) {
        super(userService);
        this.brandService = brandService;
        this.labelService = labelService;
        this.validator = validator;
        this.repository = repository;
    }

    public Uni<List<ListenerDTO>> getAllDTO(final int limit, final int offset, final IUser user, final ListenerFilter filter) {
        assert repository != null;
        return repository.getAll(limit, offset, false, user, filter)
                .chain(list -> {
                    if (list.isEmpty()) {
                        return Uni.createFrom().item(List.of());
                    } else {
                        List<Uni<ListenerDTO>> unis = list.stream()
                                .map(this::mapToDTO)
                                .collect(Collectors.toList());
                        return Uni.join().all(unis).andFailFast();
                    }
                });
    }

    public Uni<Integer> getAllCount(final IUser user, final ListenerFilter filter) {
        assert repository != null;
        return repository.getAllCount(user, false, filter);
    }

    public Uni<ListenerDTO> getDTOTemplate(IUser user, LanguageCode code) {
        return brandService.getAll(10, 0, user)
                .onItem().transform(userRadioStations -> {
                    ListenerDTO dto = new ListenerDTO();
                    dto.setAuthor(user.getUserName());
                    dto.setLastModifier(user.getUserName());
                    dto.getLocalizedName().put(LanguageCode.en, "");
                    dto.getNickName().put(LanguageCode.en, Set.of());
                    return dto;
                });
    }

    @Override
    public Uni<ListenerDTO> getDTO(UUID uuid, IUser user, LanguageCode code) {
        assert repository != null;
        return repository.findById(uuid, user, false)
                .chain(this::mapToDTO);
    }

    public Uni<ListenerMixdeckDTO> getMixdeckDTOBySlug(String slugName, IUser user) {
        assert repository != null;
        return repository.findBySlugName(slugName, user, false).chain(this::mapToMixdeckDTO);
    }

    public Uni<ListenerMixdeckDTO> getNewMixdeckDTO(IUser user, LanguageCode code) {
        return getDTOTemplate(user, code).chain(this::toMixdeckDTO);
    }

    public Uni<List<BrandListenerMixdeckDTO>> getBrandListenersMixdeck(String brandName, int limit, final int offset,
                                                                       IUser user, ListenerFilter filter) {
        return getBrandListeners(brandName, limit, offset, user, filter)
                .chain(list -> {
                    if (list.isEmpty()) {
                        return Uni.createFrom().item(List.of());
                    }
                    List<Uni<BrandListenerMixdeckDTO>> unis = list.stream()
                            .map(this::toBrandListenerMixdeckDTO)
                            .collect(Collectors.toList());
                    return Uni.join().all(unis).andFailFast();
                });
    }

    /** Mixdeck upsert; path key is listener slugName (user login), not UUID. */
    public Uni<ListenerMixdeckDTO> upsertMixdeck(String slugName, ListenerMixdeckDTO mixdeckDto, String stationSlug,
                                                 IUser user) {
        boolean isNew = DocumentIds.isNewDocumentId(slugName);
        return toLabelIds(mixdeckDto.getLabels())
                .chain(labelIds -> {
                    ListenerDTO dto = fromMixdeckDTO(mixdeckDto);
                    dto.setLabels(labelIds);
                    if (isNew) {
                        return upsert("new", dto, stationSlug, user);
                    }
                    assert repository != null;
                    return repository.findBySlugName(slugName, user, false)
                            .chain(existing -> upsert(existing.getId().toString(), dto, stationSlug, user));
                })
                .chain(this::toMixdeckDTO);
    }

    public Uni<Integer> archiveBySlug(String slugName, IUser user) {
        assert repository != null;
        return repository.findBySlugName(slugName, user, false)
                .chain(existing -> archive(existing.getId().toString(), user));
    }

    public Uni<List<BrandListenerDTO>> getBrandListeners(String brandName, int limit, final int offset, IUser user, ListenerFilter filter) {
        assert repository != null;
        assert brandService != null;

        if (brandName == null || brandName.isBlank()) {
            return getAllDTO(limit, offset, user, filter)
                    .map(list -> list.stream().map(listenerDTO -> {
                        BrandListenerDTO dto = new BrandListenerDTO();
                        dto.setId(listenerDTO.getId());
                        dto.setListenerDTO(listenerDTO);
                        return dto;
                    }).collect(Collectors.toList()));
        }

        return repository.findForBrand(brandName, limit, offset, user, false, filter)
                .chain(list -> {
                    if (list.isEmpty()) {
                        return Uni.createFrom().item(List.of());
                    } else {
                        List<Uni<BrandListenerDTO>> unis = list.stream()
                                .map(this::mapToBrandListenerDTO)
                                .collect(Collectors.toList());
                        return Uni.join().all(unis).andFailFast();
                    }

                });
    }

    public Uni<Integer> getCountBrandListeners(final String brand, final IUser user, final ListenerFilter filter) {
        assert repository != null;
        if (brand == null || brand.isBlank()) {
            return getAllCount(user, filter);
        }
        return repository.findForBrandCount(brand, user, false, filter);
    }

    public Uni<ListenerDTO> upsert(String id, ListenerDTO dto, String stationSlug, IUser user) {
        assert brandService != null;
        assert repository != null;
        Listener listener = buildEntity(dto);
        List<RlsActionDTO> rlsActions = dto.getRlsActions() != null ? dto.getRlsActions() : List.of();

        if ("new".equalsIgnoreCase(id) || id == null || id.isBlank()) {
            if (stationSlug == null) {
                return ensureUserExists(listener, dto.getEmail())
                        .chain(userId -> {
                            listener.setUserId(userId);
                            return repository.insert(listener, dto.getListenerOf(), rlsActions, user);
                        })
                        .chain(this::mapToDTO);
            } else {
                return getBrand(stationSlug)
                        .chain(station -> ensureUserExists(listener, dto.getEmail())
                                .chain(userId -> {
                                    listener.setUserId(userId);
                                    return repository.insert(listener, List.of(station.getId()), rlsActions, user);
                                }))
                        .chain(this::mapToDTO);
            }
        } else {
            UUID listenerUUID = UUID.fromString(id);
            if (stationSlug == null) {
                return repository.update(listenerUUID, listener, dto.getListenerOf(), rlsActions, user)
                        .chain(updatedListener -> {
                            if (dto.getEmail() != null && !dto.getEmail().isEmpty()) {
                                return userService.updateEmail(dto.getUserId(), dto.getEmail(), user)
                                        .replaceWith(updatedListener);
                            }
                            return Uni.createFrom().item(updatedListener);
                        })
                        .chain(this::mapToDTO);
            } else {
                return getBrand(stationSlug)
                        .chain(station -> repository.getBrandsForListener(listenerUUID)
                                .chain(stationIds -> {
                                    return repository.update(listenerUUID, listener, stationIds, rlsActions, user);
                                }))
                        .chain(this::mapToDTO);
            }
        }
    }

    private Uni<Brand> getBrand(String stationSlug) {
        return brandService.getBySlugName(stationSlug)
                .chain(station -> {
                    if (station == null) {
                        return Uni.createFrom().failure(new IllegalArgumentException("Station not found: " + stationSlug));
                    }
                    return Uni.createFrom().item(station);
                });
    }

    private Uni<Long> ensureUserExists(Listener listener, String email) {
        return userService.findByEmail(email)
                .chain(existingUser -> {
                    if (existingUser.getId() != UndefinedUser.ID) {
                        return Uni.createFrom().item(existingUser.getId());
                    }
                    return createNewUser(listener, email);
                });
    }

    private Uni<Long> createNewUser(Listener listener, String email) {
        UserDTO userDTO = new UserDTO();
        String preferredName = null;
        if (listener.getUserData() != null && listener.getUserData().getData() != null) {
            preferredName = listener.getUserData().getData().get("preferred_name");
        }
        userDTO.setLogin(WebHelper.generateUserLogin(
                preferredName, listener.getNickName(), listener.getLocalizedName()));
        userDTO.setEmail(email);
        return userService.add(userDTO, true);
    }

    private Uni<ListenerDTO> mapToDTO(Listener doc) {
        assert repository != null;
        return Uni.combine().all().unis(
                userService.getUserName(doc.getAuthor()),
                userService.getUserName(doc.getLastModifier()),
                repository.getBrandsForListener(doc.getId()),
                userService.get(doc.getUserId())
        ).asTuple().map(tuple -> {
            ListenerDTO dto = new ListenerDTO();
            dto.setId(doc.getId());
            dto.setAuthor(tuple.getItem1());
            dto.setRegDate(doc.getRegDate());
            dto.setLastModifier(tuple.getItem2());
            dto.setLastModifiedDate(doc.getLastModifiedDate());
            dto.setUserId(doc.getUserId());
            dto.setLocalizedName(doc.getLocalizedName());
            dto.setNickName(doc.getNickName());
            if (doc.getUserData() != null) {
                dto.setUserData(doc.getUserData().getData());
            }
            List<UUID> brandIds = tuple.getItem3();
            dto.setListenerOf(brandIds);
            dto.setLabels(doc.getLabels());
            Optional<IUser> userOptional = tuple.getItem4();
            userOptional.ifPresent(user -> {
                dto.setEmail(user.getEmail());
                dto.setSlugName(user.getLogin());
            });
            return dto;
        });
    }

    private Listener buildEntity(ListenerDTO dto) {
        Listener doc = new Listener();
        doc.setLocalizedName(dto.getLocalizedName());
        doc.setNickName(dto.getNickName());
        if (dto.getUserData() != null && !dto.getUserData().isEmpty()) {
            doc.setUserData(new UserData(dto.getUserData()));
        }
        if (dto.getListenerOf() != null) {
            doc.setListenerOf(dto.getListenerOf());
        }
        if (dto.getLabels() != null) {
            doc.setLabels(dto.getLabels());
        }
        return doc;
    }

    private Uni<BrandListenerDTO> mapToBrandListenerDTO(BrandListener brandListener) {
        return mapToDTO(brandListener.getListener())
                .onItem().transform(listenerDTO -> {
                    BrandListenerDTO dto = new BrandListenerDTO();
                    dto.setId(brandListener.getId());
                    dto.setListenerDTO(listenerDTO);
                    return dto;
                });
    }

    private Uni<BrandListenerMixdeckDTO> toBrandListenerMixdeckDTO(BrandListenerDTO src) {
        return toMixdeckDTO(src.getListenerDTO()).map(listener -> {
            BrandListenerMixdeckDTO dto = new BrandListenerMixdeckDTO();
            dto.setListenerDTO(listener);
            return dto;
        });
    }

    private Uni<ListenerMixdeckDTO> mapToMixdeckDTO(Listener doc) {
        return mapToDTO(doc).chain(this::toMixdeckDTO);
    }

    private Uni<ListenerMixdeckDTO> toMixdeckDTO(ListenerDTO src) {
        return toLabelIdentifiers(src.getLabels()).map(labels -> {
            ListenerMixdeckDTO dto = new ListenerMixdeckDTO();
            dto.setAuthor(src.getAuthor());
            dto.setRegDate(src.getRegDate());
            dto.setLastModifier(src.getLastModifier());
            dto.setLastModifiedDate(src.getLastModifiedDate());
            dto.setLocalizedName(src.getLocalizedName());
            dto.setNickName(src.getNickName());
            dto.setUserData(src.getUserData());
            dto.setEmail(src.getEmail());
            dto.setSlugName(src.getSlugName());
            dto.setLabels(labels);
            return dto;
        });
    }

    private ListenerDTO fromMixdeckDTO(ListenerMixdeckDTO src) {
        ListenerDTO dto = new ListenerDTO();
        dto.setLocalizedName(src.getLocalizedName());
        dto.setNickName(src.getNickName());
        dto.setUserData(src.getUserData());
        dto.setEmail(src.getEmail());
        dto.setSlugName(src.getSlugName());
        return dto;
    }

    private Uni<List<UUID>> toLabelIds(List<String> identifiers) {
        if (identifiers == null || identifiers.isEmpty()) {
            return Uni.createFrom().item(List.of());
        }
        assert labelService != null;
        List<Uni<UUID>> unis = identifiers.stream()
                .map(identifier -> labelService.findByIdentifier(identifier).map(Label::getId))
                .collect(Collectors.toList());
        return Uni.join().all(unis).andFailFast();
    }

    private Uni<List<String>> toLabelIdentifiers(List<UUID> ids) {
        if (ids == null || ids.isEmpty()) {
            return Uni.createFrom().item(List.of());
        }
        assert labelService != null;
        List<Uni<String>> unis = ids.stream()
                .map(id -> labelService.getById(id).map(Label::getIdentifier))
                .collect(Collectors.toList());
        return Uni.join().all(unis).andFailFast();
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

    public Uni<Integer> archive(String id, IUser user) {
        assert repository != null;
        return repository.archive(UUID.fromString(id), user);
    }
}