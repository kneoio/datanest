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
import com.semantyca.datanest.dto.sharing.ShareAdminDTO;
import com.semantyca.datanest.dto.sharing.ShareDTO;
import com.semantyca.datanest.dto.sharing.ReceivedSharePublicDTO;
import com.semantyca.datanest.dto.sharing.SharingPreviewDTO;
import com.semantyca.datanest.messaging.CommandPublisher;
import com.semantyca.datanest.repository.soundfragment.SharedSoundFragmentRepository;
import com.semantyca.datanest.repository.soundfragment.SoundFragmentRepository;
import com.semantyca.datanest.service.BrandService;
import com.semantyca.mixpla.dto.queue.command.CommandType;
import com.semantyca.mixpla.model.cnst.ApprovalStatus;
import com.semantyca.mixpla.model.cnst.SubmissionPolicy;
import com.semantyca.mixpla.model.soundfragment.SharedSoundFragment;
import com.semantyca.mixpla.model.soundfragment.SoundFragment;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jspecify.annotations.NonNull;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

@ApplicationScoped
public class SharedSoundFragmentService extends AbstractService<SharedSoundFragment, ShareDTO> {

    private final SharedSoundFragmentRepository repository;
    private final SoundFragmentRepository soundFragmentRepository;
    private final BrandService brandService;
    private final CommandPublisher commandPublisher;

    @Inject
    public SharedSoundFragmentService(UserService userService,
                                      SharedSoundFragmentRepository repository,
                                      SoundFragmentRepository soundFragmentRepository,
                                      BrandService brandService,
                                      CommandPublisher commandPublisher) {
        super(userService);
        this.repository = repository;
        this.soundFragmentRepository = soundFragmentRepository;
        this.brandService = brandService;
        this.commandPublisher = commandPublisher;
    }

    public Uni<Integer> rejectShareByReceiver(UUID shareId, IUser user) {
        return repository.rejectByReceiver(shareId, user.getId());
    }

    public Uni<Integer> archiveRejectedShareByReceiver(UUID shareId, IUser user) {
        return repository.archiveByReceiver(shareId, user.getId());
    }

    public Uni<Integer> acceptShareByReceiver(UUID shareId, IUser user) {
        return repository.acceptByReceiver(shareId, user.getId())
                .invoke(result -> {
                    if (result.rowsAffected() > 0) {
                        commandPublisher.publishCommand(
                                CommandType.REBUILD_AGENDA,
                                "share_accepted",
                                Map.of("brandId", result.targetBrandId().toString(), "soundFragmentId", result.soundFragmentId().toString())
                        );
                    }
                })
                .onItem().transform(SharedSoundFragmentRepository.AcceptResult::rowsAffected);
    }

    public Uni<Integer> delete(UUID shareId, IUser user) {
        return repository.archive(shareId);
    }

    // 42next admin single-share upsert - distinct from patchShares (Mixdeck's add/remove list
    // for a fragment across brands). "new"/blank id creates (or upserts onto the existing
    // natural-key row, see unique_brand_shared_fragment); otherwise updates the mutable fields
    // of the share at that id. See SharedSoundFragmentRepository#insert/update.
    public Uni<ShareAdminDTO> upsert(String id, ShareAdminDTO dto) {
        SharedSoundFragment entity = buildEntity(dto);

        if ("new".equalsIgnoreCase(id) || id == null || id.isBlank()) {
            return repository.insert(entity)
                    .chain(repository::findById)
                    .map(this::toShareAdminDTO);
        }
        UUID shareId = UUID.fromString(id);
        return repository.update(shareId, entity)
                .chain(count -> repository.findById(shareId))
                .map(this::toShareAdminDTO);
    }

    private static @NonNull SharedSoundFragment buildEntity(ShareAdminDTO dto) {
        SharedSoundFragment entity = new SharedSoundFragment();
        entity.setSourceUserId(dto.getSourceUserId());
        entity.setSourceUserName(dto.getSourceUserName());
        entity.setSourceUserEmail(dto.getSourceUserEmail());
        entity.setTargetBrandId(dto.getTargetBrandId());
        entity.setSoundFragmentId(dto.getSoundFragmentId());
        entity.setExpiresAt(dto.getExpiresAt());
        entity.setBoost(dto.getBoost());
        entity.setStatus(dto.getStatus() != null ? dto.getStatus() : ApprovalStatus.PENDING.value());
        entity.setNotifyOnPlay(dto.isNotifyOnPlay());
        return entity;
    }

    public Uni<ShareAdminDTO> getByIdAdmin(UUID id) {
        return repository.findById(id).map(this::toShareAdminDTO);
    }

    public Uni<List<ShareAdminDTO>> getAllAdmin(int limit, int offset) {
        return repository.getAllAdmin(limit, offset)
                .map(list -> list.stream().map(this::toShareAdminDTO).collect(Collectors.toList()));
    }

    public Uni<Integer> getAllAdminCount() {
        return repository.getAllAdminCount();
    }

    private ShareAdminDTO toShareAdminDTO(SharedSoundFragment e) {
        ShareAdminDTO dto = new ShareAdminDTO();
        dto.setId(e.getId());
        dto.setSourceUserId(e.getSourceUserId());
        dto.setSourceUserName(e.getSourceUserName());
        dto.setSourceUserEmail(e.getSourceUserEmail());
        dto.setNotifyOnPlay(Boolean.TRUE.equals(e.getNotifyOnPlay()));
        dto.setTargetBrandId(e.getTargetBrandId());
        dto.setSoundFragmentId(e.getSoundFragmentId());
        dto.setExpiresAt(e.getExpiresAt());
        dto.setBoost(e.getBoost());
        dto.setStatus(e.getStatus());
        return dto;
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
                                        String submitterName, String submitterEmail, boolean notifyOnPlay) {
        SharedSoundFragment entity = new SharedSoundFragment();
        entity.setSourceUserId(submitterUserId);
        entity.setSourceUserName(submitterName);
        entity.setSourceUserEmail(submitterEmail);
        entity.setTargetBrandId(targetBrandId);
        entity.setSoundFragmentId(soundFragmentId);
        entity.setNotifyOnPlay(notifyOnPlay);
        entity.setStatus(ApprovalStatus.PENDING.value());
        return repository.applyPatch(soundFragmentId, List.of(), List.of(entity));
    }

    public Uni<Integer> getSharingPreviewCount(IUser user, String search) {
        return repository.getReceivedListCount(user.getId(), search);
    }

    public Uni<List<ReceivedSharePublicDTO>> getPublicSharingPreviewList(int limit, int offset, IUser user, String search) {
        return repository.getReceivedList(limit, offset, user.getId(), search)
                .chain(list -> {
                    List<UUID> soundFragmentIds = list.stream().map(SharedSoundFragment::getSoundFragmentId).collect(Collectors.toList());
                    return soundFragmentRepository.getSlugNamesByIds(soundFragmentIds)
                            .map(slugMap -> list.stream().map(e -> toPublicSharingPreviewDTO(e, slugMap)).collect(Collectors.toList()));
                });
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

    private ReceivedSharePublicDTO toPublicSharingPreviewDTO(SharedSoundFragment e, Map<UUID, String> soundFragmentSlugs) {
        ReceivedSharePublicDTO dto = new ReceivedSharePublicDTO();
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
        String soundFragmentSlug = soundFragmentSlugs.get(e.getSoundFragmentId());
        dto.setSlugName(soundFragmentSlug);
        if (e.getFileMetadataList() != null && !e.getFileMetadataList().isEmpty() && soundFragmentSlug != null) {
            dto.setUploadedFiles(e.getFileMetadataList().stream().map(meta -> {
                UploadFileDTO fileDto = new UploadFileDTO();
                fileDto.setId(meta.getSlugName());
                fileDto.setName(meta.getFileOriginalName());
                fileDto.setUrl("/datanest/public/soundfragments/files/" + soundFragmentSlug + "/" + meta.getSlugName());
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
