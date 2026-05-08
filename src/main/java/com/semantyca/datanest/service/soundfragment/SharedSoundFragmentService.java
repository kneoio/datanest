package com.semantyca.datanest.service.soundfragment;

import com.semantyca.core.model.user.IUser;
import com.semantyca.datanest.dto.SharedSoundFragmentDTO;
import com.semantyca.datanest.model.soundfragment.SharedSoundFragment;
import com.semantyca.datanest.repository.soundfragment.SharedSoundFragmentRepository;
import com.semantyca.datanest.service.BrandService;
import com.semantyca.mixpla.model.cnst.SubmissionPolicy;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@ApplicationScoped
public class SharedSoundFragmentService {

    private final SharedSoundFragmentRepository repository;
    private final BrandService brandService;

    @Inject
    public SharedSoundFragmentService(SharedSoundFragmentRepository repository, BrandService brandService) {
        this.repository = repository;
        this.brandService = brandService;
    }

    public Uni<Void> removeShare(UUID soundFragmentId, UUID destinationBrandId) {
        return repository.deleteBySoundFragmentAndBrand(soundFragmentId, destinationBrandId).replaceWithVoid();
    }

    /**
     * Shares a fragment with a brand only if that brand uses {@link SubmissionPolicy#NO_RESTRICTIONS} for submissions.
     */
    public Uni<Void> addShareForOpenContributionBrand(UUID soundFragmentId, UUID destinationBrandId, IUser user) {
        return brandService.getById(destinationBrandId, user)
                .onItem().transformToUni(brand -> {
                    if (brand.getSubmissionPolicy() != SubmissionPolicy.NO_RESTRICTIONS) {
                        return Uni.createFrom().failure(new IllegalArgumentException(
                                "Brand does not accept contributions without restrictions: " + destinationBrandId));
                    }
                    SharedSoundFragment entity = new SharedSoundFragment();
                    entity.setSourceBrandId(destinationBrandId);
                    entity.setSoundFragmentId(soundFragmentId);
                    return repository.insertIfNotExists(entity);
                });
    }

    public Uni<List<SharedSoundFragmentDTO>> listDTOsBySoundFragmentId(UUID soundFragmentId) {
        return repository.listBySoundFragmentId(soundFragmentId)
                .map(list -> list.stream().map(this::toDTO).collect(Collectors.toList()));
    }

    public Uni<List<SharedSoundFragmentDTO>> listDTOsBySourceBrandId(UUID sourceBrandId) {
        return repository.listBySourceBrandId(sourceBrandId)
                .map(list -> list.stream().map(this::toDTO).collect(Collectors.toList()));
    }

    public Uni<SharedSoundFragmentDTO> getDTO(UUID id) {
        return repository.findById(id).map(this::toDTO);
    }

    public Uni<SharedSoundFragmentDTO> create(SharedSoundFragmentDTO dto) {
        return repository.insert(fromDTO(dto)).map(this::toDTO);
    }

    public Uni<SharedSoundFragmentDTO> update(UUID id, SharedSoundFragmentDTO dto) {
        SharedSoundFragment entity = fromDTO(dto);
        return repository.update(id, entity).map(this::toDTO);
    }

    public Uni<Integer> delete(UUID id) {
        return repository.delete(id);
    }

    private SharedSoundFragmentDTO toDTO(SharedSoundFragment e) {
        SharedSoundFragmentDTO dto = new SharedSoundFragmentDTO();
        dto.setId(e.getId());
        dto.setSourceBrandId(e.getSourceBrandId());
        dto.setSoundFragmentId(e.getSoundFragmentId());
        dto.setExpiresAt(e.getExpiresAt());
        dto.setTotalPlayedCount(e.getTotalPlayedCount());
        dto.setTotalRatedCount(e.getTotalRatedCount());
        dto.setStatus(e.getStatus());
        dto.setArchived(e.getArchived());
        return dto;
    }

    private SharedSoundFragment fromDTO(SharedSoundFragmentDTO dto) {
        SharedSoundFragment e = new SharedSoundFragment();
        e.setId(dto.getId());
        e.setSourceBrandId(dto.getSourceBrandId());
        e.setSoundFragmentId(dto.getSoundFragmentId());
        e.setExpiresAt(dto.getExpiresAt());
        e.setTotalPlayedCount(dto.getTotalPlayedCount());
        e.setTotalRatedCount(dto.getTotalRatedCount());
        e.setStatus(dto.getStatus());
        e.setArchived(dto.getArchived());
        return e;
    }
}
