package com.semantyca.datanest.service;

import com.semantyca.core.dto.DocumentAccessDTO;
import com.semantyca.core.dto.rls.RlsActionDTO;
import com.semantyca.core.model.cnst.LanguageCode;
import com.semantyca.core.model.user.IUser;
import com.semantyca.core.service.AbstractService;
import com.semantyca.core.service.UserService;
import com.semantyca.datanest.dto.script.SceneOverrideDTO;
import com.semantyca.datanest.dto.script.ScenePromptDTO;
import com.semantyca.datanest.dto.StagePlaylistDTO;
import com.semantyca.datanest.repository.SceneOverrideRepository;
import com.semantyca.mixpla.model.PlaylistRequest;
import com.semantyca.mixpla.model.SceneOverride;
import com.semantyca.mixpla.model.ScenePrompt;
import com.semantyca.mixpla.model.cnst.PlaylistItemType;
import com.semantyca.mixpla.model.cnst.SourceType;
import com.semantyca.mixpla.model.cnst.WayOfSourcing;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@ApplicationScoped
public class SceneOverrideService extends AbstractService<SceneOverride, SceneOverrideDTO> {
    private final SceneOverrideRepository repository;

    @Inject
    public SceneOverrideService(UserService userService, SceneOverrideRepository repository) {
        super(userService);
        this.repository = repository;
    }

    public Uni<List<SceneOverrideDTO>> getAllDTO(int limit, int offset, IUser user) {
        return repository.getAll(limit, offset, false, user)
                .chain(list -> {
                    if (list.isEmpty()) {
                        return Uni.createFrom().item(List.of());
                    }
                    List<Uni<SceneOverrideDTO>> unis = list.stream().map(this::mapToDTO).collect(Collectors.toList());
                    return Uni.join().all(unis).andFailFast();
                });
    }

    public Uni<Integer> getAllCount(IUser user) {
        return repository.getAllCount(user, false);
    }

    @Override
    public Uni<SceneOverrideDTO> getDTO(UUID id, IUser user, LanguageCode language) {
        return repository.findById(id, user, false).chain(this::mapToDTO);
    }

    public Uni<SceneOverrideDTO> upsert(String id, SceneOverrideDTO dto, IUser user) {
        SceneOverride entity = buildEntity(dto);
        List<RlsActionDTO> rlsActions = dto.getRlsActions() != null ? dto.getRlsActions() : List.of();
        if ("new".equalsIgnoreCase(id) || id == null || id.isBlank()) {
            return repository.insert(entity, rlsActions, user).chain(this::mapToDTO);
        } else {
            return repository.update(UUID.fromString(id), entity, rlsActions, user).chain(this::mapToDTO);
        }
    }

    public Uni<Integer> archive(String id, IUser user) {
        return repository.archive(UUID.fromString(id), user);
    }

    @Override
    public Uni<Integer> delete(String id, IUser user) {
        return repository.delete(UUID.fromString(id), user);
    }

    public Uni<List<DocumentAccessDTO>> getDocumentAccess(UUID documentId, IUser user) {
        return repository.getDocumentAccessInfo(documentId, user)
                .onItem().transform(list -> list.stream().map(this::mapToDocumentAccessDTO).collect(Collectors.toList()));
    }

    private Uni<SceneOverrideDTO> mapToDTO(SceneOverride doc) {
        return Uni.combine().all().unis(
                userService.getUserName(doc.getAuthor()),
                userService.getUserName(doc.getLastModifier())
        ).asTuple().map(tuple -> {
            SceneOverrideDTO dto = new SceneOverrideDTO();
            dto.setId(doc.getId());
            dto.setTitle(doc.getTitle());
            dto.setAuthor(tuple.getItem1());
            dto.setRegDate(doc.getRegDate());
            dto.setLastModifier(tuple.getItem2());
            dto.setLastModifiedDate(doc.getLastModifiedDate());
            dto.setSceneId(doc.getSceneId());
            dto.setStartTime(doc.getStartTime());
            dto.setActionsData(doc.getActionsData());
            dto.setWeekdays(doc.getWeekdays());
            dto.setStagePlaylist(mapStagePlaylistToDTO(doc.getStagePlaylist()));
            return dto;
        });
    }

    private SceneOverride buildEntity(SceneOverrideDTO dto) {
        SceneOverride entity = new SceneOverride();
        entity.setTitle(dto.getTitle());
        entity.setSceneId(dto.getSceneId());
        entity.setStartTime(dto.getStartTime());
        entity.setActionsData(dto.getActionsData());
        entity.setWeekdays(dto.getWeekdays());
        entity.setStagePlaylist(mapDTOToStagePlaylist(dto.getStagePlaylist()));
        return entity;
    }

    private StagePlaylistDTO mapStagePlaylistToDTO(PlaylistRequest playlistRequest) {
        if (playlistRequest == null) {
            return null;
        }
        StagePlaylistDTO dto = new StagePlaylistDTO();
        dto.setSourcing(playlistRequest.getSourcing() != null ? List.of(playlistRequest.getSourcing().name()) : null);
        dto.setTitle(playlistRequest.getTitle());
        dto.setArtist(playlistRequest.getArtist());
        dto.setGenres(playlistRequest.getGenres());
        dto.setLabels(playlistRequest.getLabels());
        dto.setType(playlistRequest.getType() != null ? playlistRequest.getType().stream().map(Enum::name).toList() : null);
        dto.setSource(playlistRequest.getSource() != null ? playlistRequest.getSource().stream().map(Enum::name).toList() : null);
        dto.setSearchTerm(playlistRequest.getSearchTerm());
        dto.setSoundFragments(playlistRequest.getSoundFragments());
        dto.setPrompts(mapScenePromptsToDTOs(playlistRequest.getContentPrompts()));
        return dto;
    }

    private PlaylistRequest mapDTOToStagePlaylist(StagePlaylistDTO dto) {
        if (dto == null) {
            return null;
        }
        PlaylistRequest playlistRequest = new PlaylistRequest();
        playlistRequest.setSourcing(dto.getSourcing() != null && !dto.getSourcing().isEmpty() ? WayOfSourcing.valueOf(dto.getSourcing().get(0)) : null);
        playlistRequest.setTitle(dto.getTitle());
        playlistRequest.setArtist(dto.getArtist());
        playlistRequest.setGenres(dto.getGenres());
        playlistRequest.setLabels(dto.getLabels());
        playlistRequest.setType(dto.getType() != null ? dto.getType().stream().map(PlaylistItemType::valueOf).toList() : null);
        playlistRequest.setSource(dto.getSource() != null ? dto.getSource().stream().map(SourceType::valueOf).toList() : null);
        playlistRequest.setSearchTerm(dto.getSearchTerm());
        playlistRequest.setSoundFragments(dto.getSoundFragments());
        playlistRequest.setContentPrompts(dto.getPrompts() != null ? mapScenePromptDTOsToEntities(dto.getPrompts()) : List.of());
        return playlistRequest;
    }

    private List<ScenePromptDTO> mapScenePromptsToDTOs(List<ScenePrompt> prompts) {
        if (prompts == null) {
            return null;
        }
        return prompts.stream().map(sp -> {
            ScenePromptDTO dto = new ScenePromptDTO();
            dto.setPromptId(sp.getPromptId());
            dto.setRank(sp.getRank());
            dto.setWeight(sp.getWeight());
            dto.setActive(sp.isActive());
            dto.setMandatory(sp.isMandatory());
            return dto;
        }).collect(Collectors.toList());
    }

    private List<ScenePrompt> mapScenePromptDTOsToEntities(List<ScenePromptDTO> dtos) {
        if (dtos == null) {
            return List.of();
        }
        return dtos.stream().map(dto -> {
            ScenePrompt sp = new ScenePrompt();
            sp.setPromptId(dto.getPromptId());
            sp.setRank(dto.getRank());
            sp.setWeight(dto.getWeight());
            sp.setActive(dto.isActive());
            sp.setMandatory(dto.isMandatory());
            return sp;
        }).collect(Collectors.toList());
    }
}
