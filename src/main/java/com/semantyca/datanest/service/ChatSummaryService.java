package com.semantyca.datanest.service;

import com.semantyca.core.model.user.IUser;
import com.semantyca.core.service.AbstractService;
import com.semantyca.core.service.UserService;
import com.semantyca.datanest.dto.ChatSummaryDTO;
import com.semantyca.datanest.model.ChatSummary;
import com.semantyca.datanest.repository.ChatSummaryRepository;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@ApplicationScoped
public class ChatSummaryService extends AbstractService<ChatSummary, ChatSummaryDTO> {

    private final ChatSummaryRepository repository;

    @Inject
    public ChatSummaryService(UserService userService, ChatSummaryRepository repository) {
        super(userService);
        this.repository = repository;
    }

    public Uni<List<ChatSummaryDTO>> getAll(final int limit, final int offset) {
        return repository.getAll(limit, offset)
                .chain(list -> {
                    if (list.isEmpty()) {
                        return Uni.createFrom().item(List.of());
                    } else {
                        List<Uni<ChatSummaryDTO>> unis = list.stream()
                                .map(this::mapToDTO)
                                .collect(Collectors.toList());
                        return Uni.join().all(unis).andFailFast();
                    }
                });
    }

    public Uni<Integer> getAllCount() {
        return repository.getAllCount();
    }

    public Uni<ChatSummaryDTO> getById(UUID id) {
        return repository.findById(id).chain(this::mapToDTO);
    }

    public Uni<List<ChatSummaryDTO>> getByBrandName(final String brandName, final int limit, final int offset) {
        return repository.findByBrandName(brandName, limit, offset)
                .chain(list -> {
                    if (list.isEmpty()) {
                        return Uni.createFrom().item(List.of());
                    } else {
                        List<Uni<ChatSummaryDTO>> unis = list.stream()
                                .map(this::mapToDTO)
                                .collect(Collectors.toList());
                        return Uni.join().all(unis).andFailFast();
                    }
                });
    }

    public Uni<Integer> getCountByBrandName(String brandName) {
        return repository.getCountByBrandName(brandName);
    }

    public Uni<Integer> delete(String id, IUser user) {
        return repository.delete(UUID.fromString(id));
    }

    private Uni<ChatSummaryDTO> mapToDTO(ChatSummary chatSummary) {
        return Uni.createFrom().item(() -> {
            ChatSummaryDTO dto = new ChatSummaryDTO();
            dto.setId(chatSummary.getId());
            dto.setBrandName(chatSummary.getBrandName());
            dto.setSummaryType(chatSummary.getSummaryType());
            dto.setUserId(chatSummary.getUserId());
            dto.setChatType(chatSummary.getChatType());
            dto.setSummary(chatSummary.getSummary());
            dto.setMessageCount(chatSummary.getMessageCount());
            dto.setPeriodStart(chatSummary.getPeriodStart());
            dto.setPeriodEnd(chatSummary.getPeriodEnd());
            dto.setCreatedAt(chatSummary.getCreatedAt());
            return dto;
        });
    }
}
