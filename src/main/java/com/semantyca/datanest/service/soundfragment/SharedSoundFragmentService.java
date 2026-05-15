package com.semantyca.datanest.service.soundfragment;

import com.semantyca.core.dto.DocumentAccessDTO;
import com.semantyca.core.model.cnst.LanguageCode;
import com.semantyca.core.model.user.IUser;
import com.semantyca.core.model.user.SuperUser;
import com.semantyca.core.service.AbstractService;
import com.semantyca.core.service.UserService;
import com.semantyca.datanest.dto.SharedSoundDTO;
import com.semantyca.datanest.dto.SharedSoundFragmentDTO;
import com.semantyca.datanest.dto.SharedSoundFragmentPatchDTO;
import com.semantyca.datanest.dto.SharedSoundFragmentPreviewDTO;
import com.semantyca.datanest.model.soundfragment.SharedSoundFragment;
import com.semantyca.datanest.repository.soundfragment.SharedSoundFragmentRepository;
import com.semantyca.datanest.repository.soundfragment.SoundFragmentRepository;
import com.semantyca.datanest.service.BrandService;
import com.semantyca.mixpla.model.brand.Brand;
import com.semantyca.mixpla.model.cnst.SubmissionPolicy;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@ApplicationScoped
public class SharedSoundFragmentService extends AbstractService<SharedSoundFragment, SharedSoundFragmentDTO> {

    private final SharedSoundFragmentRepository repository;
    private final SoundFragmentRepository soundFragmentRepository;
    private final BrandService brandService;

    @Inject
    public SharedSoundFragmentService(UserService userService,
                                      SharedSoundFragmentRepository repository,
                                      SoundFragmentRepository soundFragmentRepository,
                                      BrandService brandService) {
        super(userService);
        this.repository = repository;
        this.soundFragmentRepository = soundFragmentRepository;
        this.brandService = brandService;
    }

    public Uni<List<SharedSoundDTO>> getShared(int limit, int offset, IUser user) {
        return repository.getSharedCount(limit, offset, user.getId())
                .chain(list -> {
                    if (list.isEmpty()) {
                        return Uni.createFrom().item(List.of());
                    }
                    List<Uni<SharedSoundDTO>> unis = list.stream()
                            .map(this::toContributionDTO)
                            .collect(Collectors.toList());
                    return Uni.join().all(unis).andFailFast();
                });
    }

    public Uni<Integer> getSharedCount(IUser user) {
        return repository.getSharedCount(user.getId());
    }

    public Uni<Integer> rejectShareByReceiver(UUID shareId, IUser user) {
        return repository.deleteByIdAndReader(shareId, user.getId());
    }

    public Uni<List<SharedSoundFragmentPreviewDTO>> getReceivedList(int limit, int offset, IUser user) {
        return repository.getReceivedList(limit, offset, user.getId())
                .map(list -> list.stream().map(this::toPreviewDTO).collect(Collectors.toList()));
    }

    public Uni<Integer> getReceivedListCount(IUser user) {
        return repository.getReceivedListCount(user.getId());
    }

    public Uni<SharedSoundFragmentPreviewDTO> getById(UUID id, IUser user) {
        return repository.findById(id, user.getId()).map(this::toPreviewDTO);
    }

    public Uni<Void> patchShares(UUID fragmentId, SharedSoundFragmentPatchDTO patch, IUser user) {
        List<UUID> remove = patch.getRemoveTargetBrandIds() != null ? patch.getRemoveTargetBrandIds() : List.of();
        List<UUID> add = patch.getAddTargetBrandIds() != null ? patch.getAddTargetBrandIds() : List.of();
        boolean incognito = patch.isStayIncognito();

        if (add.isEmpty()) {
            return repository.applyPatch(fragmentId, remove, List.of());
        }

        return soundFragmentRepository.getBrandsForSoundFragment(fragmentId, user)
                .chain(brandIds -> {
                    if (brandIds.isEmpty()) {
                        return Uni.createFrom().failure(new IllegalArgumentException(
                                "Sound fragment has no associated brand: " + fragmentId));
                    }
                    return brandService.getById(brandIds.getFirst(), SuperUser.build());
                })
                .chain(sourceBrand -> validateAndBuildEntities(fragmentId, add, sourceBrand, incognito))
                .chain(entities -> repository.applyPatch(fragmentId, remove, entities));
    }

    private Uni<List<SharedSoundFragment>> validateAndBuildEntities(UUID fragmentId, List<UUID> targetBrandIds,
                                                                     Brand sourceBrand, boolean stayIncognito) {
        List<Uni<SharedSoundFragment>> unis = targetBrandIds.stream()
                .map(targetBrandId -> brandService.getById(targetBrandId, SuperUser.build())
                        .onItem().transformToUni(targetBrand -> {
                            if (targetBrand.getSubmissionPolicy() != SubmissionPolicy.NO_RESTRICTIONS) {
                                return Uni.createFrom().failure(new IllegalArgumentException(
                                        "Brand does not accept contributions without restrictions: " + targetBrandId));
                            }
                            SharedSoundFragment entity = new SharedSoundFragment();
                            entity.setSourceUserId(sourceBrand.getOwner().getUserId());
                            if (!stayIncognito) {
                                entity.setSourceUserName(sourceBrand.getOwner().getName());
                                entity.setSourceUserEmail(sourceBrand.getOwner().getEmail());
                            }
                            entity.setTargetBrandId(targetBrandId);
                            entity.setSoundFragmentId(fragmentId);
                            entity.setStatus(500);
                            return Uni.createFrom().item(entity);
                        }))
                .collect(Collectors.toList());
        return Uni.join().all(unis).andFailFast();
    }

    public Uni<List<SharedSoundFragmentDTO>> listSharedSoundFragmentDTO(UUID soundFragmentId) {
        return repository.listBySoundFragmentId(soundFragmentId)
                .map(list -> list.stream().map(this::toDTO).collect(Collectors.toList()));
    }

    @Override
    public Uni<SharedSoundFragmentDTO> getDTO(UUID id, IUser user, LanguageCode language) {
        return repository.findById(id).map(this::toDTO);
    }

    public Uni<List<DocumentAccessDTO>> getDocumentAccess(UUID documentId, IUser user) {
        return repository.getDocumentAccessInfo(documentId, user)
                .onItem().transform(accessInfoList ->
                        accessInfoList.stream()
                                .map(this::mapToDocumentAccessDTO)
                                .collect(Collectors.toList())
                );
    }

    private Uni<SharedSoundDTO> toContributionDTO(SharedSoundFragment e) {
        return listSharedSoundFragmentDTO(e.getSoundFragmentId()).map(shares -> {
            SharedSoundDTO dto = new SharedSoundDTO();
            dto.setId(e.getSoundFragmentId());
            dto.setTitle(e.getTitle());
            dto.setArtist(e.getArtist());
            dto.setType(e.getType());
            dto.setAlbum(e.getAlbum());
            dto.setGenres(e.getGenres());
            dto.setLabels(e.getLabels());
            dto.setShares(shares);
            return dto;
        });
    }

    private SharedSoundFragmentPreviewDTO toPreviewDTO(SharedSoundFragment e) {
        SharedSoundFragmentPreviewDTO dto = new SharedSoundFragmentPreviewDTO();
        dto.setId(e.getId());
        dto.setTitle(e.getTitle());
        dto.setArtist(e.getArtist());
        dto.setType(e.getType());
        dto.setAlbum(e.getAlbum());
        dto.setGenres(e.getGenres());
        dto.setLabels(e.getLabels());
        dto.setSharerUserName(e.getSourceUserName());
        dto.setSharerUserEmail(e.getSourceUserEmail());
        dto.setTargetBrandName(e.getTargetBrandName());
        return dto;
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
