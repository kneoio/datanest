package com.semantyca.datanest.service;

import com.semantyca.core.dto.DocumentAccessDTO;
import com.semantyca.core.model.cnst.LanguageCode;
import com.semantyca.core.model.cnst.LanguageTag;
import com.semantyca.core.model.user.IUser;
import com.semantyca.core.service.AbstractService;
import com.semantyca.core.service.UserService;
import com.semantyca.datanest.dto.PromptDTO;
import com.semantyca.core.dto.rls.RlsActionDTO;
import com.semantyca.datanest.repository.prompt.PromptRepository;
import com.semantyca.mixpla.model.DjPrompt;
import com.semantyca.mixpla.model.filter.PromptFilter;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

@ApplicationScoped
public class PromptService extends AbstractService<DjPrompt, PromptDTO> {
    private final PromptRepository repository;

    @Inject
    public PromptService(UserService userService, PromptRepository repository) {
        super(userService);
        this.repository = repository;
    }

    public Uni<List<PromptDTO>> getAllDTO(final int limit, final int offset, final IUser user, final PromptFilter filter) {
        return repository.getAll(limit, offset, false, user, filter)
                .chain(list -> {
                    if (list.isEmpty()) {
                        return Uni.createFrom().item(List.of());
                    } else {
                        List<Uni<PromptDTO>> unis = list.stream()
                                .map(this::mapToDTO)
                                .collect(Collectors.toList());
                        return Uni.join().all(unis).andFailFast();
                    }
                });
    }

    public Uni<List<PromptDTO>> getAllGroupedDTO(final int limit, final int offset, final IUser user, final PromptFilter filter) {
        return repository.getAllMasters(limit, offset, false, user, filter)
                .chain(masters -> {
                    if (masters.isEmpty()) {
                        return Uni.createFrom().item(List.of());
                    }
                    List<UUID> masterIds = masters.stream().map(DjPrompt::getId).toList();
                    return repository.findChildrenByMasterIds(masterIds, user)
                            .chain(children -> {
                                List<Uni<PromptDTO>> masterUnis = masters.stream()
                                        .map(this::mapToDTO)
                                        .collect(Collectors.toList());
                                return Uni.join().all(masterUnis).andFailFast()
                                        .chain(masterDtos -> {
                                            if (children.isEmpty()) {
                                                return Uni.createFrom().item(masterDtos);
                                            }
                                            List<Uni<PromptDTO>> childUnis = children.stream()
                                                    .map(this::mapToDTO)
                                                    .collect(Collectors.toList());
                                            return Uni.join().all(childUnis).andFailFast()
                                                    .map(childDtos -> {
                                                        Map<UUID, List<PromptDTO>> byMaster = new HashMap<>();
                                                        for (int i = 0; i < children.size(); i++) {
                                                            UUID mid = children.get(i).getMasterId();
                                                            if (mid != null) {
                                                                byMaster.computeIfAbsent(mid, k -> new ArrayList<>())
                                                                        .add(childDtos.get(i));
                                                            }
                                                        }
                                                        for (PromptDTO p : masterDtos) {
                                                            List<PromptDTO> ch = byMaster.get(p.getId());
                                                            if (ch != null && !ch.isEmpty()) {
                                                                ch.sort(Comparator.comparing(
                                                                        PromptDTO::getLanguageTag,
                                                                        Comparator.nullsLast(String::compareTo)));
                                                                p.setChildren(ch);
                                                            }
                                                        }
                                                        return masterDtos;
                                                    });
                                        });
                            });
                });
    }

    public Uni<Integer> getAllMastersCount(final IUser user, final PromptFilter filter) {
        return repository.getAllMastersCount(user, false, filter);
    }

    public Uni<List<DjPrompt>> getAll(final int limit, final int offset, final IUser user, final PromptFilter filter) {
        return repository.getAll(limit, offset, false, user, filter);
    }

    public Uni<Integer> getAllCount(final IUser user, final PromptFilter filter) {
        return repository.getAllCount(user, false, filter);
    }

    public Uni<DjPrompt> getById(UUID id, IUser user) {
        return repository.findById(id, user, false);
    }

    public Uni<List<DjPrompt>> getByIds(List<UUID> ids, IUser user) {
        return repository.findByIds(ids, user);
    }

    @Override
    public Uni<PromptDTO> getDTO(UUID id, IUser user, LanguageCode language) {
        return repository.findById(id, user, false).chain(this::mapToDTO);
    }

    public Uni<PromptDTO> upsert(String id, PromptDTO dto, IUser user) {
        DjPrompt entity = buildEntity(dto);
        List<RlsActionDTO> rlsActions = dto.getRlsActions() != null ? dto.getRlsActions() : List.of();
        if ("new".equalsIgnoreCase(id) || id == null || id.isBlank()) {
            return repository.insert(entity, rlsActions, user).chain(this::mapToDTO);
        } else {
            return repository.update(UUID.fromString(id), entity, rlsActions, user).chain(this::mapToDTO);
        }
    }

    public Uni<DjPrompt> insert(DjPrompt entity, IUser user) {
        return repository.insert(entity, user);
    }

    public Uni<DjPrompt> update(UUID id, DjPrompt entity, IUser user) {
        return repository.update(id, entity, user);
    }

    public Uni<List<JsonObject>> getOptions() {
        return repository.getAllOptions()
                .map(list -> list.stream()
                        .map(p -> new JsonObject()
                                .put("id", p.getId().toString())
                                .put("optionLocName", p.getOptionLocName() != null
                                        ? p.getOptionLocName().getString("en", "")
                                        : ""))
                        .collect(Collectors.toList()));
    }

    public Uni<Integer> archive(String id, IUser user) {
        return repository.archive(UUID.fromString(id), user);
    }

    @Override
    public Uni<Integer> delete(String id, IUser user) {
        return repository.delete(UUID.fromString(id), user);
    }

    public Uni<DjPrompt> findByMasterAndLanguage(UUID masterId, LanguageTag languageCode, boolean includeArchived) {
        return repository.findByMasterAndLanguage(masterId, languageCode, includeArchived);
    }

    private Uni<PromptDTO> mapToDTO(DjPrompt doc) {
        return Uni.combine().all().unis(
                userService.getUserName(doc.getAuthor()),
                userService.getUserName(doc.getLastModifier())
        ).asTuple().map(tuple -> {
            PromptDTO dto = new PromptDTO();
            dto.setId(doc.getId());
            dto.setAuthor(tuple.getItem1());
            dto.setRegDate(doc.getRegDate());
            dto.setLastModifier(tuple.getItem2());
            dto.setLastModifiedDate(doc.getLastModifiedDate());
            dto.setEnabled(doc.isEnabled());
            dto.setPrompt(doc.getPrompt());
            dto.setDescription(doc.getDescription());
            dto.setPromptType(doc.getPromptType());
            dto.setLanguageTag(doc.getLanguageTag().tag());
            dto.setMaster(doc.isMaster());
            dto.setLocked(doc.isLocked());
            dto.setTitle(doc.getTitle());
            dto.setBackup(doc.getBackup());
            dto.setDraftId(doc.getDraftId());
            dto.setMasterId(doc.getMasterId());
            dto.setVersion(doc.getVersion());
            dto.setAllowAsOption(doc.getAllowAsOption());
            dto.setOptionLocName(doc.getOptionLocName());
            dto.setExposedVariables(doc.getExposedVariables());
            return dto;
        });
    }

    private DjPrompt buildEntity(PromptDTO dto) {
        DjPrompt doc = new DjPrompt();
        doc.setId(dto.getId());
        doc.setEnabled(dto.isEnabled());
        doc.setPrompt(dto.getPrompt());
        doc.setDescription(dto.getDescription());
        doc.setPromptType(dto.getPromptType());
        doc.setLanguageTag(LanguageTag.fromTag(dto.getLanguageTag()));
        doc.setMaster(dto.isMaster());
        doc.setLocked(dto.isLocked());
        doc.setTitle(dto.getTitle());
        doc.setBackup(dto.getBackup());
        doc.setDraftId(dto.getDraftId());
        if (dto.isMaster()) {
            doc.setMasterId(null);
        } else {
            doc.setMasterId(dto.getMasterId());
        }
        doc.setVersion(dto.getVersion());
        doc.setAllowAsOption(dto.getAllowAsOption());
        doc.setOptionLocName(dto.getOptionLocName());
        doc.setExposedVariables(dto.getExposedVariables() != null ? dto.getExposedVariables() : new io.vertx.core.json.JsonArray());
        return doc;
    }

    public Uni<List<DocumentAccessDTO>> getDocumentAccess(UUID documentId, IUser user) {
        return repository.getDocumentAccessInfo(documentId, user)
                .onItem().transform(accessInfoList ->
                        accessInfoList.stream()
                                .map(this::mapToDocumentAccessDTO)
                                .collect(Collectors.toList())
                );
    }
}
