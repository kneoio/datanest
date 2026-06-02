package com.semantyca.datanest.service;

import com.semantyca.core.dto.DocumentAccessDTO;
import com.semantyca.core.model.cnst.LanguageCode;
import com.semantyca.core.model.user.IUser;
import com.semantyca.core.service.AbstractService;
import com.semantyca.core.service.UserService;
import com.semantyca.datanest.dto.ArtistDTO;
import com.semantyca.datanest.model.artist.Artist;
import com.semantyca.datanest.repository.ArtistRepository;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.EnumMap;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@ApplicationScoped
public class ArtistService extends AbstractService<Artist, ArtistDTO> {
    private static final Logger LOGGER = Logger.getLogger(ArtistService.class);

    private final ArtistRepository repository;

    @Inject
    public ArtistService(UserService userService, ArtistRepository repository) {
        super(userService);
        this.repository = repository;
    }

    public Uni<List<ArtistDTO>> getAll(int limit, int offset, IUser user) {
        return repository.getAll(limit, offset, false, user)
                .chain(list -> {
                    if (list.isEmpty()) return Uni.createFrom().item(List.of());
                    List<Uni<ArtistDTO>> unis = list.stream().map(this::mapToDTO).collect(Collectors.toList());
                    return Uni.join().all(unis).andFailFast();
                });
    }

    public Uni<Integer> getAllCount(IUser user) {
        return repository.getAllCount(user, false);
    }

    @Override
    public Uni<ArtistDTO> getDTO(UUID uuid, IUser user, LanguageCode language) {
        return repository.findById(uuid, user, false).chain(this::mapToDTO);
    }

    public Uni<ArtistDTO> upsert(String id, ArtistDTO dto, IUser user) {
        Artist entity = buildEntity(dto);
        List<com.semantyca.core.dto.rls.RlsActionDTO> rlsActions =
                dto.getRlsActions() != null ? dto.getRlsActions() : List.of();

        Uni<Artist> op;
        if ("new".equalsIgnoreCase(id) || id == null || id.isBlank()) {
            op = repository.insert(entity, rlsActions, user);
        } else {
            op = repository.update(UUID.fromString(id), entity, rlsActions, user);
        }
        return op.chain(this::mapToDTO);
    }

    public Uni<Integer> archive(String id, IUser user) {
        return repository.archive(UUID.fromString(id), user);
    }

    public Uni<List<DocumentAccessDTO>> getDocumentAccess(UUID documentId, IUser user) {
        return repository.getDocumentAccessInfo(documentId, user)
                .onItem().transform(list -> list.stream()
                        .map(this::mapToDocumentAccessDTO)
                        .collect(Collectors.toList()));
    }

    private Uni<ArtistDTO> mapToDTO(Artist doc) {
        return Uni.combine().all().unis(
                userService.getUserName(doc.getAuthor()),
                userService.getUserName(doc.getLastModifier())
        ).asTuple().map(tuple -> {
            ArtistDTO dto = new ArtistDTO();
            dto.setId(doc.getId());
            dto.setAuthor(tuple.getItem1());
            dto.setRegDate(doc.getRegDate());
            dto.setLastModifier(tuple.getItem2());
            dto.setLastModifiedDate(doc.getLastModifiedDate());
            dto.setLocalizedName(doc.getLocalizedName() != null ? new EnumMap<>(doc.getLocalizedName()) : null);
            dto.setCountry(doc.getCountry());
            dto.setStyles(doc.getStyles());
            dto.setDescription(doc.getDescription());
            dto.setLinks(doc.getLinks());
            dto.setUserData(doc.getUserData());
            return dto;
        });
    }

    private Artist buildEntity(ArtistDTO dto) {
        Artist artist = new Artist();
        if (dto.getLocalizedName() != null) {
            artist.setLocalizedName(new EnumMap<>(dto.getLocalizedName()));
        }
        artist.setCountry(dto.getCountry());
        artist.setStyles(dto.getStyles());
        artist.setDescription(dto.getDescription());
        artist.setLinks(dto.getLinks());
        artist.setUserData(dto.getUserData());
        return artist;
    }
}
