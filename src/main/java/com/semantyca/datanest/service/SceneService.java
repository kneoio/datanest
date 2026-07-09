package com.semantyca.datanest.service;

import com.semantyca.core.dto.DocumentAccessDTO;
import com.semantyca.core.dto.rls.RlsActionDTO;
import com.semantyca.core.model.cnst.LanguageCode;
import com.semantyca.core.model.user.IUser;
import com.semantyca.core.service.AbstractService;
import com.semantyca.core.service.UserService;
import com.semantyca.datanest.dto.PlaylistRequestDTO;
import com.semantyca.datanest.dto.script.AbsoluteSceneDTO;
import com.semantyca.datanest.dto.script.AbstractSceneDTO;
import com.semantyca.datanest.dto.script.CustomActionDTO;
import com.semantyca.datanest.dto.script.RelativeSceneDTO;
import com.semantyca.datanest.dto.script.ScenePromptDTO;
import com.semantyca.datanest.repository.SceneRepository;
import com.semantyca.mixpla.model.CustomAction;
import com.semantyca.mixpla.model.PlaylistRequest;
import com.semantyca.mixpla.model.Scene;
import com.semantyca.mixpla.model.ScenePrompt;
import com.semantyca.mixpla.model.cnst.PlaylistItemType;
import com.semantyca.mixpla.model.cnst.SceneTimingMode;
import com.semantyca.mixpla.model.cnst.SourceType;
import com.semantyca.mixpla.model.cnst.WayOfSourcing;
import com.semantyca.mixpla.model.filter.SceneFilter;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@ApplicationScoped
public class SceneService extends AbstractService<Scene, AbstractSceneDTO> {
    private final SceneRepository repository;

    @Inject
    public SceneService(UserService userService, SceneRepository repository) {
        super(userService);
        this.repository = repository;
    }

    public Uni<List<AbstractSceneDTO>> getAllDTO(final int limit, final int offset, final IUser user, SceneFilter filter) {
        return repository.getAll(limit, offset, false, user, filter)
                .chain(list -> {
                    if (list.isEmpty()) {
                        return Uni.createFrom().item(List.of());
                    }
                    List<Uni<AbstractSceneDTO>> unis = list.stream().map(this::mapToDTO).collect(Collectors.toList());
                    return Uni.join().all(unis).andFailFast();
                });
    }

    public Uni<Integer> getAllCount(final IUser user, SceneFilter filter) {
        return repository.getAllCount(user, false, filter);
    }

    public Uni<List<Scene>> getAllWithPromptIds(final UUID scriptId, final int limit, final int offset, final IUser user) {
        return repository.listByScript(scriptId, limit, offset, false, user);
    }

    public Uni<List<AbstractSceneDTO>> getAllByScript(final UUID scriptId, final int limit, final int offset, final IUser user) {
        return repository.listByScript(scriptId, limit, offset, false, user)
                .chain(list -> {
                    if (list.isEmpty()) {
                        return Uni.createFrom().item(List.of());
                    }
                    List<Uni<AbstractSceneDTO>> unis = list.stream().map(this::mapToDTO).collect(Collectors.toList());
                    return Uni.join().all(unis).andFailFast();
                });
    }

    @Override
    public Uni<AbstractSceneDTO> getDTO(UUID id, IUser user, LanguageCode language) {
        return repository.findById(id, user, false).chain(this::mapToDTO);
    }

    public Uni<Scene> getById(UUID sceneId, IUser user) {
        return repository.findById(sceneId, user, false);
    }

    public Uni<AbstractSceneDTO> upsert(String id, AbstractSceneDTO dto, IUser user) {
        Scene entity = buildEntity(dto);
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

    private Uni<AbstractSceneDTO> mapToDTO(Scene doc) {
        return Uni.combine().all().unis(
                userService.getUserName(doc.getAuthor()),
                userService.getUserName(doc.getLastModifier())
        ).asTuple().map(tuple -> {
            AbstractSceneDTO dto;
            if (doc.getTimingMode() == SceneTimingMode.RELATIVE_TO_STREAM_START) {
                RelativeSceneDTO relativeDto = new RelativeSceneDTO();
                relativeDto.setSeqNum(doc.getSeqNum());
                relativeDto.setDurationSeconds(doc.getDurationSeconds());
                dto = relativeDto;
            } else {
                AbsoluteSceneDTO absoluteDto = new AbsoluteSceneDTO();
                absoluteDto.setStartTime(doc.getStartTime());
                absoluteDto.setWeekdays(doc.getWeekdays());
                absoluteDto.setOneTimeRun(doc.isOneTimeRun());
                dto = absoluteDto;
            }
            dto.setTimingMode(doc.getTimingMode());
            dto.setId(doc.getId());
            dto.setTitle(doc.getTitle());
            dto.setScriptTitle(doc.getScriptTitle());
            dto.setAuthor(tuple.getItem1());
            dto.setRegDate(doc.getRegDate());
            dto.setLastModifier(tuple.getItem2());
            dto.setLastModifiedDate(doc.getLastModifiedDate());
            dto.setScriptId(doc.getScriptId());
            dto.setTalkativity(doc.getTalkativity());
            dto.setAllowJingles(doc.isAllowJingles());
            dto.setAllowAds(doc.isAllowAds());
            dto.setPrompts(mapScenePromptsToDTOs(doc.getIntroPrompts()));
            dto.setActions(mapCustomActionsToDTOs(doc.getActions()));
            dto.setPlaylistRequest(mapStagePlaylistToDTO(doc.getPlaylistRequest()));
            return dto;
        });
    }

    private List<CustomActionDTO> mapCustomActionsToDTOs(List<CustomAction> actions) {
        if (actions == null || actions.isEmpty()) {
            return null;
        }
        return actions.stream()
                .map(a -> {
                    CustomActionDTO dto = new CustomActionDTO();
                    dto.setName(a.getName());
                    dto.setInstruction(a.getInstruction());
                    dto.setContextVars(CustomActionDTO.AVAILABLE_CONTEXT_VARS);
                    return dto;
                })
                .collect(Collectors.toList());
    }

    private List<ScenePromptDTO> mapScenePromptsToDTOs(List<ScenePrompt> livePrompts) {
        if (livePrompts == null) {
            return null;
        }
        return livePrompts.stream()
                .map(sp -> {
                    ScenePromptDTO dto = new ScenePromptDTO();
                    dto.setPromptId(sp.getPromptId());
                    dto.setRank(sp.getRank());
                    dto.setWeight(sp.getWeight());
                    dto.setActive(sp.isActive());
                    dto.setMandatory(sp.isMandatory());
                    return dto;
                })
                .collect(Collectors.toList());
    }

    private Scene buildEntity(AbstractSceneDTO dto) {
        Scene entity = new Scene();
        entity.setTitle(dto.getTitle());
        entity.setTimingMode(dto.getTimingMode());
        if (dto instanceof RelativeSceneDTO relativeDto) {
            entity.setSeqNum(relativeDto.getSeqNum());
            entity.setDurationSeconds(relativeDto.getDurationSeconds());
        } else if (dto instanceof AbsoluteSceneDTO absoluteDto) {
            entity.setStartTime(absoluteDto.getStartTime());
            entity.setWeekdays(absoluteDto.getWeekdays());
            entity.setOneTimeRun(absoluteDto.isOneTimeRun());
        }
        entity.setTalkativity(dto.getTalkativity());
        entity.setAllowJingles(dto.isAllowJingles());
        entity.setAllowAds(dto.isAllowAds());
        entity.setIntroPrompts(dto.getPrompts() != null ? mapScenePromptDTOsToEntities(dto.getPrompts()) : List.of());
        entity.setPlaylistRequest(mapToPlaylistRequestDTO(dto.getPlaylistRequest()));
        entity.setScriptId(dto.getScriptId());

        if (entity.getPlaylistRequest() != null && entity.getPlaylistRequest().getSourcing() == WayOfSourcing.GENERATED) {
            entity.setOneTimeRun(true);
        }
        
        return entity;
    }

    private List<ScenePrompt> mapScenePromptDTOsToEntities(List<ScenePromptDTO> dtos) {
        if (dtos == null) {
            return List.of();
        }
        return dtos.stream()
                .map(dto -> {
                    ScenePrompt sp = new ScenePrompt();
                    sp.setPromptId(dto.getPromptId());
                    sp.setRank(dto.getRank());
                    sp.setWeight(dto.getWeight());
                    sp.setActive(dto.isActive());
                    sp.setMandatory(dto.isMandatory());
                    return sp;
                })
                .collect(Collectors.toList());
    }

    public Uni<List<DocumentAccessDTO>> getDocumentAccess(UUID documentId, IUser user) {
        return repository.getDocumentAccessInfo(documentId, user)
                .onItem().transform(accessInfoList -> accessInfoList.stream().map(this::mapToDocumentAccessDTO).collect(Collectors.toList()));
    }

    private PlaylistRequestDTO mapStagePlaylistToDTO(PlaylistRequest doc) {
        if (doc == null) {
            return null;
        }
        PlaylistRequestDTO dto = new PlaylistRequestDTO();
        dto.setMixingType(doc.getMixingType());
        dto.setMixingArtefacts(doc.getMixingArtefacts());
        dto.setSourcing(doc.getSourcing() != null ? doc.getSourcing().name() : null);
        dto.setTitle(doc.getTitle());
        dto.setArtist(doc.getArtist());
        dto.setGenres(doc.getGenres());
        dto.setLabels(doc.getLabels());
        dto.setType(doc.getType() != null ? doc.getType().stream().map(Enum::name).toList() : null);
        dto.setSource(doc.getSource() != null ? doc.getSource().stream().map(Enum::name).toList() : null);
        dto.setSearchTerm(doc.getSearchTerm());
        dto.setSoundFragments(doc.getSoundFragments());
        dto.setPrompts(mapScenePromptsToDTOs(doc.getContentPrompts()));
        return dto;
    }

    private PlaylistRequest mapToPlaylistRequestDTO(PlaylistRequestDTO dto) {
        if (dto == null) {
            return null;
        }
        PlaylistRequest doc = new PlaylistRequest();
        doc.setMixingType(dto.getMixingType());
        doc.setMixingArtefacts(dto.getMixingArtefacts());
        doc.setSourcing(dto.getSourcing() != null ? WayOfSourcing.valueOf(dto.getSourcing()) : null);
        doc.setTitle(dto.getTitle());
        doc.setArtist(dto.getArtist());
        doc.setGenres(dto.getGenres());
        doc.setLabels(dto.getLabels());
        doc.setType(dto.getType() != null ? dto.getType().stream().map(PlaylistItemType::valueOf).toList() : null);
        doc.setSource(dto.getSource() != null ? dto.getSource().stream().map(SourceType::valueOf).toList() : null);
        doc.setSearchTerm(dto.getSearchTerm());
        doc.setSoundFragments(dto.getSoundFragments());
        doc.setContentPrompts(dto.getPrompts() != null ? mapScenePromptDTOsToEntities(dto.getPrompts()) : List.of());
        return doc;
    }
}
