package com.semantyca.datanest.service.soundfragment;

import com.semantyca.core.dto.DocumentAccessDTO;
import com.semantyca.core.model.cnst.FileType;
import com.semantyca.core.model.cnst.LanguageCode;
import com.semantyca.core.model.user.IUser;
import com.semantyca.core.model.user.SuperUser;
import com.semantyca.core.service.AbstractService;
import com.semantyca.core.service.UserService;
import com.semantyca.datanest.dto.SharePatchDTO;
import com.semantyca.datanest.dto.UploadFileDTO;
import com.semantyca.datanest.dto.sharing.ShareDTO;
import com.semantyca.datanest.dto.sharing.SharingPreviewDTO;
import com.semantyca.datanest.model.cnst.ApprovalStatus;
import com.semantyca.datanest.repository.soundfragment.SharedSoundFragmentRepository;
import com.semantyca.datanest.repository.soundfragment.SoundFragmentRepository;
import com.semantyca.datanest.service.BrandService;
import com.semantyca.mixpla.model.cnst.SubmissionPolicy;
import com.semantyca.mixpla.model.soundfragment.SharedSoundFragment;
import com.semantyca.mixpla.model.soundfragment.SoundFragment;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@ApplicationScoped
public class SharedSoundFragmentService extends AbstractService<SharedSoundFragment, ShareDTO> {

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

    public Uni<Integer> rejectShareByReceiver(UUID shareId, IUser user) {
        return repository.rejectByReceiver(shareId, user.getId());
    }

    public Uni<Integer> archiveRejectedShareByReceiver(UUID shareId, IUser user) {
        return repository.archiveByReceiver(shareId, user.getId());
    }

    public Uni<Integer> acceptShareByReceiver(UUID shareId, IUser user) {
        return repository.acceptByReceiver(shareId, user.getId());
    }

    public Uni<Integer> delete(UUID shareId, IUser user) {
        return repository.archive(shareId);
    }

    public Uni<Integer> archiveBySoundFragmentId(UUID soundFragmentId) {
        return repository.archiveBySoundFragmentId(soundFragmentId);
    }

    public Uni<Void> deleteBySoundFragmentId(UUID soundFragmentId) {
        return repository.deleteBySoundFragmentId(soundFragmentId);
    }

    // Creates a PENDING share from a submitter (an artist, resolved to a real core user account
    // by SoundFragmentService.resolveSubmitterAccount) to a target station. This is how a public/
    // chat contribution becomes visible to a station owner — via the exact same mechanism as an
    // inter-station share, not a separate approval system. See SHARING_WORKFLOW.md. The underlying
    // fragment has no brand association until this is accepted, so it shows up for the submitter's
    // own account as "unassigned to brands" in the meantime.
    public Uni<Void> shareContribution(UUID soundFragmentId, UUID targetBrandId, Long submitterUserId,
                                        String submitterName, String submitterEmail) {
        SharedSoundFragment entity = new SharedSoundFragment();
        entity.setSourceUserId(submitterUserId);
        entity.setSourceUserName(submitterName);
        entity.setSourceUserEmail(submitterEmail);
        entity.setTargetBrandId(targetBrandId);
        entity.setSoundFragmentId(soundFragmentId);
        entity.setStatus(ApprovalStatus.PENDING.value());
        return repository.applyPatch(soundFragmentId, List.of(), List.of(entity));
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

    // Slug names the source station attributing the share (its owner's name/email is recorded as
    // sourceUserName/Email, shown to the receiver as "shared by"). A fragment with no brand
    // association (e.g. the "unassigned to brands" page) has no station to pick - the FE sends
    // this sentinel instead, and the sharer is just the current user directly.
    public static final String NO_BRAND_SLUG = "NO_BRAND";

    public Uni<Void> patchShares(UUID fragmentId, String slug, SharePatchDTO patch, IUser user) {
        List<UUID> remove = patch.getRemoveTargetBrandIds() != null ? patch.getRemoveTargetBrandIds() : List.of();
        List<UUID> add = patch.getAddTargetBrandIds() != null ? patch.getAddTargetBrandIds() : List.of();
        boolean incognito = patch.isStayIncognito();
        if (add.isEmpty()) {
            return repository.applyPatch(fragmentId, remove, List.of());
        }

        boolean notifyOnPlay = patch.isNotifyOnPlay();
        Uni<SoundFragment> fragmentUni = soundFragmentRepository.findById(fragmentId, user.getId(), false, true, false);
        Uni<List<SharedSoundFragment>> entitiesUni = NO_BRAND_SLUG.equals(slug)
                ? fragmentUni.chain(fragment -> validateAndBuildEntities(fragment, add, user.getId(), user.getUserName(), user.getEmail(), incognito, notifyOnPlay))
                : fragmentUni.chain(fragment -> brandService.getBySlugNameForUser(slug, user)
                        .chain(sourceBrand -> validateAndBuildEntities(fragment, add,
                                sourceBrand.getOwner().getUserId(), sourceBrand.getOwner().getName(),
                                sourceBrand.getOwner().getEmail(), incognito, notifyOnPlay)));
        return entitiesUni.chain(entities -> repository.applyPatch(fragmentId, remove, entities));
    }

    private Uni<List<SharedSoundFragment>> validateAndBuildEntities(SoundFragment fragment, List<UUID> targetBrandIds,
                                                                     Long sourceUserId, String sourceUserName, String sourceUserEmail,
                                                                     boolean stayIncognito, boolean notifyOnPlay) {
        List<Uni<SharedSoundFragment>> unis = targetBrandIds.stream()
                .map(targetBrandId -> brandService.getById(targetBrandId, SuperUser.build())
                        .onItem().transformToUni(targetBrand -> {
                            if (targetBrand.getSubmissionPolicy() != SubmissionPolicy.NO_RESTRICTIONS) {
                                return Uni.createFrom().failure(new IllegalArgumentException(
                                        "Brand does not accept contributions without restrictions: " + targetBrandId));
                            }
                            SharedSoundFragment entity = new SharedSoundFragment();
                            entity.setSourceUserId(sourceUserId);
                            if (!stayIncognito) {
                                entity.setSourceUserName(sourceUserName);
                                entity.setSourceUserEmail(sourceUserEmail);
                            }
                            entity.setTargetBrandId(targetBrandId);
                            entity.setSoundFragmentId(fragment.getId());
                            entity.setNotifyOnPlay(notifyOnPlay);
                            // Every new share starts PENDING regardless of genre fit — no automatic
                            // accept/reject decision. Genre stays visible to the reviewing station
                            // owner as context (rendered as tags), it's not an automated gate.
                            entity.setStatus(ApprovalStatus.PENDING.value());
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
        dto.setBoost(e.getBoost() != null ? e.getBoost() : 0);
        dto.setStatus(e.getStatus());
        dto.setNotifyOnPlay(Boolean.TRUE.equals(e.getNotifyOnPlay()));
        if (e.getFileMetadataList() != null && !e.getFileMetadataList().isEmpty()) {
            dto.setUploadedFiles(e.getFileMetadataList().stream().map(meta -> {
                UploadFileDTO fileDto = new UploadFileDTO();
                fileDto.setId(meta.getSlugName());
                fileDto.setName(meta.getFileOriginalName());
                fileDto.setUrl("/soundfragments/files/" + e.getSoundFragmentId() + "/" + meta.getSlugName());
                fileDto.setType(meta.getFileType() == FileType.OPUS_ENCODED_SOUND_FRAGMENT ? "opus" : "original");
                return fileDto;
            }).collect(Collectors.toList()));
        }
        return dto;
    }

    private ShareDTO toDTO(SharedSoundFragment e) {
        ShareDTO dto = new ShareDTO();
        dto.setStatus(e.getStatus());
        dto.setShared(e.getStatus() == null || e.getStatus() != ApprovalStatus.REJECTED.value());
        dto.setNotifyOnPlay(Boolean.TRUE.equals(e.getNotifyOnPlay()));
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

    public Uni<Void> updateBoost(UUID sharedFragmentId, int boost) {
        return repository.updateBoost(sharedFragmentId, boost);
    }

}
