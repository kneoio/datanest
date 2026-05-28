package com.semantyca.datanest.service;

import com.semantyca.core.model.UserData;
import com.semantyca.core.model.cnst.LanguageCode;
import com.semantyca.core.model.user.IUser;
import com.semantyca.core.service.AbstractService;
import com.semantyca.core.service.UserService;
import com.semantyca.datanest.dto.UserAdDTO;
import com.semantyca.datanest.repository.UserAdRepository;
import com.semantyca.mixpla.model.UserAd;
import com.semantyca.mixpla.model.filter.UserAdFilter;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@ApplicationScoped
public class UserAdService extends AbstractService<UserAd, UserAdDTO> {
    private final UserAdRepository repository;

    @Inject
    public UserAdService(UserService userService, UserAdRepository repository) {
        super(userService);
        this.repository = repository;
    }

    public Uni<List<UserAdDTO>> getAll(int limit, int offset, IUser user, UserAdFilter filter) {
        return repository.getAll(limit, offset, false, user, filter)
                .chain(list -> {
                    if (list.isEmpty()) {
                        return Uni.createFrom().item(List.of());
                    }
                    List<Uni<UserAdDTO>> unis = list.stream().map(this::mapToDTO).collect(Collectors.toList());
                    return Uni.join().all(unis).andFailFast();
                });
    }

    public Uni<Integer> getAllCount(IUser user, UserAdFilter filter) {
        return repository.getAllCount(user, false, filter);
    }

    @Override
    public Uni<UserAdDTO> getDTO(UUID id, IUser user, LanguageCode language) {
        return repository.findById(id, false).chain(this::mapToDTO);
    }

    public Uni<UserAdDTO> upsert(String id, UserAdDTO dto, IUser user) {
        UserAd entity = buildEntity(dto);
        if ("new".equalsIgnoreCase(id) || id == null || id.isBlank()) {
            return repository.insert(entity, user).chain(this::mapToDTO);
        } else {
            return repository.update(UUID.fromString(id), entity, user).chain(this::mapToDTO);
        }
    }

    @Override
    public Uni<Integer> delete(String id, IUser user) {
        return repository.archive(UUID.fromString(id), user);
    }

    private Uni<UserAdDTO> mapToDTO(UserAd doc) {
        return Uni.combine().all().unis(
                userService.getUserName(doc.getAuthor()),
                userService.getUserName(doc.getLastModifier())
        ).asTuple().map(tuple -> {
            UserAdDTO dto = new UserAdDTO();
            dto.setId(doc.getId());
            dto.setAuthor(tuple.getItem1());
            dto.setRegDate(doc.getRegDate());
            dto.setLastModifier(tuple.getItem2());
            dto.setLastModifiedDate(doc.getLastModifiedDate());
            dto.setUserId(doc.getUserId());
            dto.setTitle(doc.getTitle());
            dto.setDescription(doc.getDescription());
            dto.setContacts(doc.getContacts());
            dto.setArchived(doc.getArchived());
            if (doc.getUserData() != null) {
                dto.setUserData(doc.getUserData().getData());
            }
            return dto;
        });
    }

    private UserAd buildEntity(UserAdDTO dto) {
        UserAd entity = new UserAd();
        entity.setUserId(dto.getUserId());
        entity.setTitle(dto.getTitle());
        entity.setDescription(dto.getDescription());
        entity.setContacts(dto.getContacts());
        if (dto.getUserData() != null && !dto.getUserData().isEmpty()) {
            entity.setUserData(new UserData(dto.getUserData()));
        }
        return entity;
    }
}
