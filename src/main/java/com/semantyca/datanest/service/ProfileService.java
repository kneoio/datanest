package com.semantyca.datanest.service;

import com.semantyca.core.dto.DocumentAccessDTO;
import com.semantyca.core.dto.rls.RlsActionDTO;
import com.semantyca.core.model.cnst.LanguageCode;
import com.semantyca.core.model.user.IUser;
import com.semantyca.core.service.AbstractService;
import com.semantyca.core.service.UserService;
import com.semantyca.core.util.WebHelper;
import com.semantyca.datanest.dto.ProfileDTO;
import com.semantyca.datanest.dto.ProfileFlatDTO;
import com.semantyca.datanest.dto.brand.mixdeck.ProfileMixdeckDTO;
import com.semantyca.datanest.repository.ProfileRepository;
import com.semantyca.mixpla.model.Profile;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@ApplicationScoped
public class ProfileService extends AbstractService<Profile, ProfileDTO> {

    private final ProfileRepository repository;


    @Inject
    public ProfileService(UserService userService, ProfileRepository repository) {
        super(userService);
        this.repository = repository;
    }

    public Uni<List<ProfileDTO>> getAll(final int limit, final int offset, final IUser user) {
        return repository.getAll(limit, offset, false, user)
                .chain(list -> {
                    if (list.isEmpty()) {
                        return Uni.createFrom().item(List.of());
                    } else {
                        List<Uni<ProfileDTO>> unis = list.stream()
                                .map(this::mapToDTO)
                                .collect(Collectors.toList());
                        return Uni.join().all(unis).andFailFast();
                    }
                });
    }

    public Uni<List<ProfileFlatDTO>> getAllFlat(final int limit, final int offset, final IUser user) {
        return repository.getAll(limit, offset, false, user)
                .map(list -> list.stream().map(this::mapToFlatDTO).collect(Collectors.toList()));
    }

    private ProfileFlatDTO mapToFlatDTO(Profile profile) {
        ProfileFlatDTO dto = new ProfileFlatDTO();
        dto.setId(profile.getId());
        dto.setName(profile.getName());
        dto.setDescription(profile.getDescription());
        dto.setExplicitContent(profile.isExplicitContent());
        return dto;
    }

    public Uni<Integer> getAllCount(final IUser user) {
        assert repository != null;
        return repository.getAllCount(user, false);
    }

    @Override
    public Uni<ProfileDTO> getDTO(UUID id, IUser user, LanguageCode language) {
        return repository.findById(id).chain(this::mapToDTO);
    }

    public Uni<Profile> getById(UUID id) {
        return repository.findById(id);
    }

    public Uni<Profile> findByName(String name) {
        return repository.findByName(name);
    }

    public Uni<ProfileDTO> upsert(String id, ProfileDTO dto, IUser user, LanguageCode code) {
        Profile entity = buildEntity(dto);
        List<RlsActionDTO> rlsActions = dto.getRlsActions() != null ? dto.getRlsActions() : List.of();
        if ("new".equalsIgnoreCase(id) || id == null || id.isBlank()) {
            return repository.insert(entity, rlsActions, user).chain(this::mapToDTO);
        } else {
            return repository.update(UUID.fromString(id), entity, rlsActions, user).chain(this::mapToDTO);
        }
    }

    @Override
    public Uni<Integer> delete(String id, IUser user) {
        return repository.archive(UUID.fromString(id), user);
    }


    private Uni<ProfileDTO> mapToDTO(Profile profile) {
        return Uni.combine().all().unis(
                userService.getUserName(profile.getAuthor()),
                userService.getUserName(profile.getLastModifier())
        ).asTuple().map(tuple -> {
            ProfileDTO dto = new ProfileDTO();
            dto.setId(profile.getId());
            dto.setAuthor(tuple.getItem1());
            dto.setRegDate(profile.getRegDate());
            dto.setLastModifier(tuple.getItem2());
            dto.setLastModifiedDate(profile.getLastModifiedDate());
            dto.setName(profile.getName());
            dto.setDescription(profile.getDescription());
            dto.setExplicitContent(profile.isExplicitContent());
            return dto;
        });
    }

    private Profile buildEntity(ProfileDTO dto) {
        Profile entity = new Profile();
        entity.setName(dto.getName());
        entity.setSlugName(WebHelper.generateSlug(dto.getName()));
        entity.setDescription(dto.getDescription());
        entity.setExplicitContent(dto.isExplicitContent());
        return entity;
    }

    public Uni<UUID> getIdBySlug(String slugName) {
        return repository.findIdBySlugName(slugName);
    }

    public Uni<String> getSlugById(UUID id) {
        return repository.findSlugNameById(id);
    }

    public Uni<ProfileMixdeckDTO> mapToMixdeckDTO(Profile profile) {
        ProfileMixdeckDTO dto = new ProfileMixdeckDTO();
        dto.setAuthor(null);
        dto.setName(profile.getName());
        dto.setSlugName(profile.getSlugName());
        dto.setDescription(profile.getDescription());
        dto.setExplicitContent(profile.isExplicitContent());
        return Uni.createFrom().item(dto);
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