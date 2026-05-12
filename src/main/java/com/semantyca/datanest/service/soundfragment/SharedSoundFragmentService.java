package com.semantyca.datanest.service.soundfragment;

import com.semantyca.core.model.user.IUser;
import com.semantyca.datanest.dto.SharedSoundFragmentDTO;
import com.semantyca.datanest.dto.SharedSoundFragmentPreviewDTO;
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

    public Uni<Void> removeShare(UUID soundFragmentId, UUID targetBrandId) {
        return repository.deleteBySoundFragmentAndBrand(soundFragmentId, targetBrandId).replaceWithVoid();
    }

    public Uni<Integer> rejectShare(UUID soundFragmentId, IUser user) {
        return repository.deleteByFragmentIdAndReader(soundFragmentId, user.getId());
    }

    public Uni<List<SharedSoundFragmentPreviewDTO>> getPreviewList(int limit, int offset, IUser user) {
        return repository.getPreviewList(limit, offset, user.getId());
    }

    public Uni<Integer> getPreviewCount(IUser user) {
        return repository.getPreviewCount(user.getId());
    }

    public Uni<SharedSoundFragmentPreviewDTO> getPreviewById(UUID soundFragmentId, IUser user) {
        return repository.getPreviewById(soundFragmentId, user.getId());
    }

    public Uni<Void> addShareForOpenContributionBrand(UUID soundFragmentId, UUID targetBrandId, IUser user) {
        return addShareForOpenContributionBrand(soundFragmentId, targetBrandId, user, false);
    }

    public Uni<Void> addShareForOpenContributionBrand(UUID soundFragmentId, UUID targetBrandId, IUser user,
                                                       boolean stayIncognito) {
        return brandService.getById(targetBrandId, user)
                .onItem().transformToUni(brand -> {
                    if (brand.getSubmissionPolicy() != SubmissionPolicy.NO_RESTRICTIONS) {
                        return Uni.createFrom().failure(new IllegalArgumentException(
                                "Brand does not accept contributions without restrictions: " + targetBrandId));
                    }
                    SharedSoundFragment entity = new SharedSoundFragment();
                    entity.setSourceUserId(brand.getOwner().getUserId());
                    if (!stayIncognito) {
                        entity.setSourceUserName(brand.getOwner().getName());
                        entity.setSourceUserEmail(brand.getOwner().getEmail());
                    }
                    entity.setTargetBrandId(targetBrandId);
                    entity.setSoundFragmentId(soundFragmentId);
                    entity.setStatus(500);
                    return repository.insertIfNotExists(entity);
                });
    }

    public Uni<List<SharedSoundFragmentDTO>> listDTOsBySoundFragmentId(UUID soundFragmentId) {
        return repository.listBySoundFragmentId(soundFragmentId)
                .map(list -> list.stream().map(this::toDTO).collect(Collectors.toList()));
    }

    public Uni<SharedSoundFragmentDTO> getDTO(UUID id) {
        return repository.findById(id).map(this::toDTO);
    }

    private SharedSoundFragmentDTO toDTO(SharedSoundFragment e) {
        SharedSoundFragmentDTO dto = new SharedSoundFragmentDTO();
        dto.setId(e.getId());
        dto.setSourceUserId(e.getSourceUserId());
        dto.setSourceUserName(e.getSourceUserName());
        dto.setSourceUserEmail(e.getSourceUserEmail());
        dto.setTargetBrandId(e.getTargetBrandId());
        dto.setSoundFragmentId(e.getSoundFragmentId());
        dto.setExpiresAt(e.getExpiresAt());
        dto.setPlayedCount(e.getPlayedCount());
        dto.setRatedCount(e.getRatedCount());
        dto.setStatus(e.getStatus());
        dto.setArchived(e.getArchived());
        return dto;
    }
}
