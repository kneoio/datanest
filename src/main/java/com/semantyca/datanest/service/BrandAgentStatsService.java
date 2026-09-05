package com.semantyca.datanest.service;

import com.semantyca.core.model.user.IUser;
import com.semantyca.core.service.AbstractService;
import com.semantyca.core.service.UserService;
import com.semantyca.datanest.dto.BrandAgentStatsDTO;
import com.semantyca.datanest.repository.BrandAgentStatsRepository;
import com.semantyca.mixpla.model.BrandAgentStats;
import com.semantyca.mixpla.model.filter.BrandAgentStatsFilter;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.List;
import java.util.stream.Collectors;

@ApplicationScoped
public class BrandAgentStatsService extends AbstractService<BrandAgentStats, BrandAgentStatsDTO> {

    private final BrandAgentStatsRepository repository;

    protected BrandAgentStatsService() {
        super();
        this.repository = null;
    }

    @Inject
    public BrandAgentStatsService(UserService userService, BrandAgentStatsRepository repository) {
        super(userService);
        this.repository = repository;
    }

    public Uni<List<BrandAgentStatsDTO>> getAll(int limit, int offset, BrandAgentStatsFilter filter) {
        assert repository != null;
        return repository.getAll(limit, offset, filter)
                .chain(list -> {
                    if (list.isEmpty()) return Uni.createFrom().item(List.of());
                    List<Uni<BrandAgentStatsDTO>> unis = list.stream()
                            .map(this::toDTO)
                            .collect(Collectors.toList());
                    return Uni.join().all(unis).andFailFast();
                });
    }

    public Uni<Integer> getAllCount(BrandAgentStatsFilter filter) {
        assert repository != null;
        return repository.getAllCount(filter);
    }

    public Uni<BrandAgentStatsDTO> getById(Integer id) {
        assert repository != null;
        return repository.findById(id).chain(this::toDTO);
    }

    public Uni<Integer> delete(String id, IUser user) {
        assert repository != null;
        return repository.delete(Integer.parseInt(id));
    }

    private Uni<BrandAgentStatsDTO> toDTO(BrandAgentStats stats) {
        return Uni.createFrom().item(() -> {
            BrandAgentStatsDTO dto = new BrandAgentStatsDTO();
            dto.setId(stats.getId());
            dto.setStationName(stats.getStationName());
            dto.setUserAgent(stats.getUserAgent());
            dto.setIpAddress(stats.getIpAddress());
            dto.setCountryCode(stats.getCountryCode());
            dto.setAccessCount(stats.getAccessCount());
            dto.setStreamType(stats.getStreamType());
            dto.setLastAccessTime(stats.getLastAccessTime());
            return dto;
        });
    }
}
