package com.semantyca.datanest.service.soundfragment;

import com.semantyca.core.dto.DocumentAccessDTO;
import com.semantyca.core.model.cnst.LanguageCode;
import com.semantyca.core.model.user.IUser;
import com.semantyca.core.model.user.SuperUser;
import com.semantyca.core.service.AbstractService;
import com.semantyca.core.service.UserService;
import com.semantyca.datanest.dto.sharing.ShareDTO;
import com.semantyca.datanest.dto.SharePatchDTO;
import com.semantyca.datanest.dto.sharing.SharingPreviewDTO;
import com.semantyca.datanest.model.soundfragment.SharedSoundFragment;
import com.semantyca.datanest.repository.soundfragment.SharedSoundFragmentRepository;
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
public class SharedSoundFragmentService extends AbstractService<SharedSoundFragment, ShareDTO> {

    private final SharedSoundFragmentRepository repository;
    private final BrandService brandService;
    private final SoundFragmentService soundFragmentService;

    @Inject
    public SharedSoundFragmentService(UserService userService,
                                      SharedSoundFragmentRepository repository,
                                      BrandService brandService,
                                      SoundFragmentService soundFragmentService) {
        super(userService);
        this.repository = repository;
        this.brandService = brandService;
        this.soundFragmentService = soundFragmentService;
    }

    public Uni<Integer> rejectShareByReceiver(UUID shareId, IUser user) {
        return repository.rejectByReceiver(shareId, user.getId());
    }

    public Uni<Integer> delete(UUID shareId, IUser user) {
        return repository.archive(shareId);
    }

    public Uni<List<SharingPreviewDTO>> getSharingPreviewList(int limit, int offset, IUser user) {
        return repository.getReceivedList(limit, offset, user.getId())
                .map(list -> list.stream().map(this::toSharingPreviewDTO).collect(Collectors.toList()));
    }

    public Uni<Integer> getSharingPreviewCount(IUser user) {
        return repository.getReceivedListCount(user.getId());
    }

    public Uni<SharingPreviewDTO> getById(UUID id, IUser user) {
        return repository.findById(id, user.getId()).map(this::toSharingPreviewDTO);
    }

    public Uni<Void> patchShares(UUID fragmentId, String slug, SharePatchDTO patch, IUser user) {
        List<UUID> remove = patch.getRemoveTargetBrandIds() != null ? patch.getRemoveTargetBrandIds() : List.of();
        List<UUID> add = patch.getAddTargetBrandIds() != null ? patch.getAddTargetBrandIds() : List.of();
        boolean incognito = patch.isStayIncognito();

        if (add.isEmpty()) {
            return repository.applyPatch(fragmentId, remove, List.of());
        }

        return soundFragmentService.getById(fragmentId, user)
                .chain(ignored -> brandService.getBySlugNameForUser(slug, user))
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

    public Uni<List<ShareDTO>> listShareDTO(UUID soundFragmentId) {
        return repository.listBySoundFragmentId(soundFragmentId)
                .map(list -> list.stream().map(this::toDTO).collect(Collectors.toList()));
    }

    public Uni<List<UUID>> getSharedFragmentIds(List<UUID> fragmentIds) {
        return repository.hasActiveShares(fragmentIds);
    }

    @Override
    public Uni<ShareDTO> getDTO(UUID id, IUser user, LanguageCode language) {
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

    private SharingPreviewDTO toSharingPreviewDTO(SharedSoundFragment e) {
        SharingPreviewDTO dto = new SharingPreviewDTO();
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

    private ShareDTO toDTO(SharedSoundFragment e) {
        ShareDTO dto = new ShareDTO();
        dto.setStatus(e.getStatus());
        dto.setShared(e.getStatus() == null || e.getStatus() != 501);
        String brandName = null;
        if (e.getTargetBrandName() != null && !e.getTargetBrandName().isEmpty()) {
            brandName = e.getTargetBrandName().get(LanguageCode.en);
            if (brandName == null) {
                brandName = e.getTargetBrandName().values().iterator().next();
            }
        }
        dto.setTargetBrand(brandName != null ? brandName : e.getBrandSlugName());
        return dto;
    }

}
