package com.semantyca.datanest.service.soundfragment;

import com.semantyca.core.dto.DocumentAccessDTO;
import com.semantyca.core.dto.rls.RlsActionDTO;
import com.semantyca.core.dto.rls.RlsActionType;
import com.semantyca.core.dto.scheduler.OnceTriggerDTO;
import com.semantyca.core.dto.scheduler.PeriodicTriggerDTO;
import com.semantyca.core.dto.scheduler.ScheduleDTO;
import com.semantyca.core.dto.scheduler.TaskDTO;
import com.semantyca.core.model.DataEntity;
import com.semantyca.core.model.FileMetadata;
import com.semantyca.core.model.cnst.ArchivedStatus;
import com.semantyca.core.model.cnst.FileType;
import com.semantyca.core.model.cnst.LanguageCode;
import com.semantyca.core.model.cnst.TriggerType;
import com.semantyca.core.model.scheduler.OnceTrigger;
import com.semantyca.core.model.scheduler.PeriodicTrigger;
import com.semantyca.core.model.scheduler.Scheduler;
import com.semantyca.core.model.scheduler.Task;
import com.semantyca.core.model.user.IUser;
import com.semantyca.core.model.user.SuperUser;
import com.semantyca.core.service.AbstractService;
import com.semantyca.core.service.UserService;
import com.semantyca.core.util.FileSecurityUtils;
import com.semantyca.core.util.WebHelper;
import com.semantyca.datanest.config.DatanestConfig;
import com.semantyca.datanest.dto.AudioMetadataDTO;
import com.semantyca.datanest.dto.sharing.ShareDTO;
import com.semantyca.datanest.dto.SoundFragmentDTO;
import com.semantyca.datanest.dto.UploadFileDTO;
import com.semantyca.datanest.repository.soundfragment.SoundFragmentBrandRepository;
import com.semantyca.datanest.repository.soundfragment.SoundFragmentRepository;
import com.semantyca.datanest.service.BrandService;
import com.semantyca.datanest.service.RefService;
import com.semantyca.datanest.service.maintenance.LocalFileCleanupService;
import com.semantyca.datanest.service.manipulation.OpusEncodingService;
import com.semantyca.core.service.external.hetzner.HetznerStorageService;
import com.semantyca.mixpla.model.brand.Brand;
import com.semantyca.mixpla.model.brand.Owner;
import com.semantyca.mixpla.model.cnst.PlaylistItemType;
import com.semantyca.mixpla.model.cnst.SourceType;
import com.semantyca.mixpla.model.filter.SoundFragmentFilter;
import com.semantyca.mixpla.model.soundfragment.SoundFragment;
import com.semantyca.officeframe.service.GenreService;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.validation.Validator;
import org.jboss.logging.Logger;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.stream.Collectors;

@ApplicationScoped
public class SoundFragmentService extends AbstractService<SoundFragment, SoundFragmentDTO> {
    private static final Logger LOGGER = Logger.getLogger(SoundFragmentService.class);
    private static final int FREE_PLAN_SONG_LIMIT = 500;

    private final SoundFragmentRepository repository;
    private final SoundFragmentBrandRepository brandRepository;
    private final BrandService brandService;
    private final GenreService genreService;
    private final LocalFileCleanupService localFileCleanupService;
    private final RefService refService;
    private final SharedSoundFragmentService sharedSoundFragmentService;
    private final OpusEncodingService opusEncodingService;
    private final HetznerStorageService fileStorage;
    private String uploadDir;
    Validator validator;

    protected SoundFragmentService(UserService userService, GenreService genreService) {
        super(userService);
        this.genreService = genreService;
        this.localFileCleanupService = null;
        this.repository = null;
        this.brandRepository = null;
        this.brandService = null;
        this.refService = null;
        this.sharedSoundFragmentService = null;
        this.opusEncodingService = null;
        this.fileStorage = null;
    }

    @Inject
    public SoundFragmentService(UserService userService,
                                BrandService brandService, GenreService genreService,
                                LocalFileCleanupService localFileCleanupService,
                                Validator validator,
                                SoundFragmentRepository repository,
                                SoundFragmentBrandRepository brandRepository,
                                DatanestConfig config,
                                RefService refService,
                                SharedSoundFragmentService sharedSoundFragmentService,
                                OpusEncodingService opusEncodingService,
                                HetznerStorageService fileStorage) {
        super(userService);
        this.genreService = genreService;
        this.localFileCleanupService = localFileCleanupService;
        this.validator = validator;
        this.repository = repository;
        this.brandRepository = brandRepository;
        this.brandService = brandService;
        this.refService = refService;
        this.sharedSoundFragmentService = sharedSoundFragmentService;
        this.opusEncodingService = opusEncodingService;
        this.fileStorage = fileStorage;
        uploadDir = config.getPathUploads() + "/sound-fragments-controller";
    }

    public Uni<List<SoundFragmentDTO>> getAllDTO(final int limit, final int offset, final IUser user, final SoundFragmentFilter filter) {
        assert repository != null;
        return repository.getAll(limit, offset, false, user, filter)
                .chain(this::buildListDTOs);
    }

    public Uni<Integer> getAllCount(final IUser user, final SoundFragmentFilter filter) {
        assert repository != null;
        return repository.getAllCount(user, false, filter);
    }

    public Uni<List<SoundFragmentDTO>> getAllDTOWithoutBrandAssociation(final int limit, final int offset,
                                                                        final IUser user, final SoundFragmentFilter filter) {
        assert repository != null;
        return repository.getAllWithoutBrandAssociation(limit, offset, user, filter)
                .chain(this::buildListDTOs);
    }

    public Uni<Integer> getAllCountWithoutBrandAssociation(final IUser user, final SoundFragmentFilter filter) {
        assert repository != null;
        return repository.getAllCountWithoutBrandAssociation(user, filter);
    }

    public Uni<List<SoundFragmentDTO>> getAllDTO(final int limit, final int offset, final IUser user,
                                                 final SoundFragmentFilter filter, final ArchivedStatus archivedStatus) {
        assert repository != null;
        Integer archivedCode = archivedStatus != null ? archivedStatus.getCode() : null;
        return repository.getAll(limit, offset, false, user, filter, archivedCode)
                .chain(this::buildListDTOs);
    }

    public Uni<Integer> getAllCount(final IUser user, final SoundFragmentFilter filter, final ArchivedStatus archivedStatus) {
        assert repository != null;
        Integer archivedCode = archivedStatus != null ? archivedStatus.getCode() : null;
        return repository.getAllCount(user, false, filter, archivedCode);
    }

    public Uni<SoundFragment> getById(UUID uuid, IUser user) {
        assert repository != null;
        return repository.findById(uuid, user.getId(), false, true, false);
    }

    public Uni<SoundFragment> getById(UUID uuid) {
        assert repository != null;
        return repository.findById(uuid, SuperUser.ID, false, false, true);
    }

    @Override
    public Uni<SoundFragmentDTO> getDTO(UUID uuid, IUser user, LanguageCode code) {
        assert repository != null;
        Uni<SoundFragment> soundFragmentUni = repository.findById(uuid, user.getId(), false, true, true);
        Uni<List<UUID>> brandsUni = repository.getBrandsForSoundFragment(uuid, user);

        return Uni.combine().all().unis(soundFragmentUni, brandsUni).asTuple()
                .chain(tuple -> {
                    SoundFragment doc = tuple.getItem1();
                    List<UUID> representedInBrands = tuple.getItem2();
                    assert sharedSoundFragmentService != null;
                    return sharedSoundFragmentService.listShareDTO(doc.getId())
                            .chain(shared -> mapToDTO(doc, true, representedInBrands, shared));
                });
    }

    public Uni<SoundFragmentDTO> getDTOTemplate(IUser user, LanguageCode code) {
        return brandService.getAll(10, 0, user)
                .onItem().transform(userRadioStations -> {
                    SoundFragmentDTO dto = new SoundFragmentDTO();
                    dto.setAuthor(user.getUserName());
                    dto.setLastModifier(user.getUserName());
                    dto.setNewlyUploaded(List.of());
                    dto.setUploadedFiles(List.of());
                    dto.setType(PlaylistItemType.SONG);

                    List<UUID> stationIds = userRadioStations.stream()
                            .map(Brand::getId)
                            .collect(Collectors.toList());
                    dto.setRepresentedInBrands(stationIds);

                    return dto;
                });
    }

    public Uni<FileMetadata> getFileBySlugName(UUID soundFragmentId, String slugName, IUser user) {
        assert repository != null;
        return repository.getFileBySlugName(soundFragmentId, slugName, user, false);
    }

    public Uni<SoundFragmentDTO> upsert(String id, SoundFragmentDTO dto, IUser user, LanguageCode code) {
        SoundFragment entity = buildEntity(dto);

        List<FileMetadata> fileMetadataList = new ArrayList<>();
        if (dto.getNewlyUploaded() != null && !dto.getNewlyUploaded().isEmpty()) {
            for (String fileName : dto.getNewlyUploaded()) {
                String safeFileName;
                try {
                    safeFileName = FileSecurityUtils.sanitizeFilename(fileName);
                } catch (SecurityException e) {
                    LOGGER.errorf("Security violation: Unsafe filename '%s' from user: %s", fileName, user.getUserName());
                    return Uni.createFrom().failure(new IllegalArgumentException("Invalid filename: " + fileName));
                }

                String tempFolderName = "";
                FileMetadata fileMetadata = new FileMetadata();
                if (id == null || "new".equalsIgnoreCase(id)) {
                    tempFolderName = "temp";
                } else {
                    try {
                        UUID.fromString(id);
                        tempFolderName = id;
                    } catch (IllegalArgumentException e) {
                        LOGGER.errorf("Security violation: Invalid entity ID '%s' from user: %s", id, user.getUserName());
                        return Uni.createFrom().failure(new IllegalArgumentException("Invalid entity ID"));
                    }
                }

                Path secureFilePath;
                try {
                    Path baseDir = Paths.get(uploadDir, user.getUserName(), tempFolderName);
                    secureFilePath = FileSecurityUtils.secureResolve(baseDir, safeFileName);
                } catch (SecurityException e) {
                    LOGGER.errorf("Security violation: Path traversal attempt by user %s with filename %s",
                            user.getUserName(), fileName);
                    return Uni.createFrom().failure(new SecurityException("Invalid file path"));
                }

                if (!Files.exists(secureFilePath)) {
                    LOGGER.errorf("File not found for upload. expectedPath=%s user=%s", secureFilePath, user.getUserName());
                    return Uni.createFrom().failure(new IllegalArgumentException("Something happen wrong with the uploaded file"));
                }

                fileMetadata.setFilePath(secureFilePath);
                fileMetadata.setFileOriginalName(safeFileName);
                fileMetadata.setSlugName(WebHelper.generateSlug(entity.getArtist(), entity.getTitle(), entity.getAlbum()));
                fileMetadataList.add(fileMetadata);
            }
        }

        entity.setFileMetadataList(fileMetadataList);

        if ("new".equalsIgnoreCase(id) || id == null) {
            entity.setSource(SourceType.USER_UPLOAD);
            List<UUID> targetBrands = dto.getRepresentedInBrands() != null ? dto.getRepresentedInBrands() : List.of();
            return checkBrandSongLimits(targetBrands, user)
                    .chain(() -> buildRlsActionsWithCoOwners(dto.getRepresentedInBrands(), dto.getRlsActions())
                            .chain(rlsActions -> repository.insert(entity, dto.getRepresentedInBrands(), rlsActions, user))
                            .chain(doc -> moveFilesForNewEntity(doc, fileMetadataList, user))
                            .chain(doc -> mapToDTO(doc, true, null, null))
                            .onFailure().invoke(failure -> {
                                LOGGER.warnf("Entity creation failed, cleaning up temp files for user: %s", user.getUserName());
                                localFileCleanupService.cleanupTempFilesForUser(user.getUserName())
                                        .subscribe().with(
                                                ignored -> LOGGER.debug("Temp files cleaned up after failure"),
                                                cleanupError -> LOGGER.warn("Failed to cleanup temp files", cleanupError)
                                        );
                            }));
        } else {
            List<UUID> brandIds = dto.getRepresentedInBrands() != null ? dto.getRepresentedInBrands() : List.of();
            UUID brandId = brandIds.isEmpty() ? null : brandIds.get(0);
            return buildRlsActionsWithCoOwners(dto.getRepresentedInBrands(), dto.getRlsActions())
                    .chain(rlsActions -> repository.update(UUID.fromString(id), entity, dto.getRepresentedInBrands(), rlsActions, user))
                    .invoke(doc -> triggerOpusEncodingIfMissing(doc, brandId, fileMetadataList))
                    .chain(doc -> mapToDTO(doc, true, null, null))
                    .onFailure().invoke(failure -> {
                        LOGGER.warnf("Entity update failed, cleaning up files for user: %s, entity: %s",
                                user.getUserName(), id);
                        localFileCleanupService.cleanupEntityFiles(user.getUserName(), id)
                                .subscribe().with(
                                        ignored -> LOGGER.debug("Entity files cleaned up after failure"),
                                        cleanupError -> LOGGER.warn("Failed to cleanup entity files", cleanupError)
                                );
                    });
        }
    }

    private Uni<Void> checkBrandSongLimits(List<UUID> brandIds, IUser user) {
        if (brandIds.isEmpty()) {
            return Uni.createFrom().voidItem();
        }
        assert brandRepository != null;
        return Multi.createFrom().iterable(brandIds)
                .onItem().transformToUniAndConcatenate(bId ->
                        brandRepository.findForBrandCount(bId, user, null)
                                .chain(count -> count >= FREE_PLAN_SONG_LIMIT
                                        ? Uni.createFrom().<Integer>failure(new IllegalStateException(
                                                "Song limit reached: brand has reached the free plan maximum of " + FREE_PLAN_SONG_LIMIT + " songs"))
                                        : Uni.createFrom().item(count)))
                .collect().asList()
                .replaceWithVoid();
    }

    private Uni<SoundFragment> moveFilesForNewEntity(SoundFragment doc, List<FileMetadata> fileMetadataList, IUser user) {
        if (fileMetadataList.isEmpty()) {
            return Uni.createFrom().item(doc);
        }

        try {
            Path userBaseDir = Paths.get(uploadDir, user.getUserName());
            Path tempDir = userBaseDir.resolve("temp");
            Path entityDir = userBaseDir.resolve(doc.getId().toString());

            if (Files.exists(tempDir)) {
                if (!Files.exists(entityDir)) {
                    Files.createDirectories(entityDir);
                }

                for (FileMetadata meta : fileMetadataList) {
                    String safeFileName = FileSecurityUtils.sanitizeFilename(meta.getFileOriginalName());

                    Path tempFile = FileSecurityUtils.secureResolve(tempDir, safeFileName);
                    Path entityFile = FileSecurityUtils.secureResolve(entityDir, safeFileName);

                    if (!FileSecurityUtils.isPathWithinBase(tempDir, tempFile) ||
                            !FileSecurityUtils.isPathWithinBase(entityDir, entityFile)) {
                        LOGGER.errorf("Security violation: Invalid file paths during move operation for user: %s", user.getUserName());
                        return Uni.createFrom().failure(new SecurityException("Invalid file paths"));
                    }

                    if (Files.exists(tempFile)) {
                        Files.move(tempFile, entityFile, StandardCopyOption.REPLACE_EXISTING);
                        meta.setFilePath(entityFile);
                        LOGGER.debugf("Securely moved file from %s to %s for user: %s", tempFile, entityFile, user.getUserName());
                    }
                }
            }

            return Uni.createFrom().item(doc);
        } catch (SecurityException e) {
            LOGGER.errorf("Security violation during file move for entity: %s, user: %s", doc.getId(), user.getUserName(), e);
            return Uni.createFrom().failure(e);
        } catch (Exception e) {
            LOGGER.errorf("Failed to move files for entity: %s, user: %s", doc.getId(), user.getUserName(), e);
            return Uni.createFrom().failure(e);
        }
    }

    public Uni<Integer> bulkBrandUpdate(List<UUID> documentIds, List<String> brands, String operation, IUser user) {
        return getBrandUUIDs(brands)
                .chain(brandUUIDs -> {
                    List<Uni<SoundFragment>> updateUnis = documentIds.stream()
                            .map(documentId ->
                                    repository.findById(documentId, user.getId(), false, false, false)
                                            .chain(fragment -> {

                                                if (fragment == null) {
                                                    return Uni.createFrom().nullItem();
                                                }

                                                List<UUID> updatedBrandIds;
                                                if ("SET".equals(operation)) {
                                                    updatedBrandIds = new ArrayList<>(brandUUIDs);
                                                } else {
                                                    updatedBrandIds = new ArrayList<>();
                                                }

                                                return repository.update(documentId, fragment, updatedBrandIds, Collections.emptyList(), user);
                                            })
                                            .onFailure().recoverWithItem(throwable -> {
                                                LOGGER.errorf("Failed to update document %s: %s", documentId, throwable.getMessage());
                                                return null;
                                            })
                            )
                            .collect(Collectors.toList());

                    return Uni.join().all(updateUnis).andFailFast()
                            .map(results -> (int) results.stream().filter(Objects::nonNull).count());
                });
    }

    private Uni<List<UUID>> getBrandUUIDs(List<String> brandNames) {
        if (brandNames == null || brandNames.isEmpty()) {
            return Uni.createFrom().item(new ArrayList<>());
        }

        List<Uni<Brand>> stationUnis = brandNames.stream()
                .map(brandService::getBySlugName)
                .collect(Collectors.toList());

        return Uni.join().all(stationUnis).andFailFast()
                .map(stations -> stations.stream()
                        .filter(Objects::nonNull)
                        .map(Brand::getId)
                        .collect(Collectors.toList()));
    }

    private Uni<List<SoundFragmentDTO>> buildListDTOs(List<SoundFragment> list) {
        if (list.isEmpty()) return Uni.createFrom().item(List.of());
        List<UUID> ids = list.stream().map(SoundFragment::getId).collect(Collectors.toList());
        return sharedSoundFragmentService.getSharedFragmentIds(ids)
                .chain(sharedIds -> {
                    Set<UUID> sharedSet = new HashSet<>(sharedIds);
                    List<Uni<SoundFragmentDTO>> unis = list.stream()
                            .map(doc -> mapToDTO(doc, false, null, null)
                                    .map(dto -> { dto.setShared(sharedSet.contains(doc.getId())); return dto; }))
                            .collect(Collectors.toList());
                    return Uni.join().all(unis).andFailFast();
                });
    }

    private Uni<SoundFragmentDTO> mapToDTO(SoundFragment doc, boolean exposeFileUrl, List<UUID> representedInBrands,
                                           List<ShareDTO> shares) {
        return Uni.combine().all().unis(
                userService.getUserName(doc.getAuthor()),
                userService.getUserName(doc.getLastModifier())
        ).asTuple().onItem().transform(tuple -> {
            String author = tuple.getItem1();
            String lastModifier = tuple.getItem2();
            List<UploadFileDTO> files = new ArrayList<>();

            if (exposeFileUrl && doc.getFileMetadataList() != null) {
                doc.getFileMetadataList().forEach(meta -> {
                    String safeFileName = FileSecurityUtils.sanitizeFilename(meta.getFileOriginalName());
                    UploadFileDTO fileDto = new UploadFileDTO();
                    fileDto.setId(meta.getSlugName());
                    fileDto.setName(safeFileName);
                    fileDto.setStatus("finished");
                    fileDto.setUrl("/soundfragments/files/" + doc.getId() + "/" + meta.getSlugName());
                    fileDto.setPercentage(100);
                    fileDto.setType(meta.getFileType() == FileType.OPUS_ENCODED_SOUND_FRAGMENT ? "opus" : "original");
                    files.add(fileDto);
                });
            }

            SoundFragmentDTO dto = new SoundFragmentDTO();
            dto.setId(doc.getId());
            dto.setAuthor(author);
            dto.setRegDate(doc.getRegDate());
            dto.setLastModifier(lastModifier);
            dto.setLastModifiedDate(doc.getLastModifiedDate());
            dto.setSource(doc.getSource());
            dto.setStatus(doc.getStatus());
            dto.setType(doc.getType());
            dto.setTitle(doc.getTitle());
            dto.setArtist(doc.getArtist());
            dto.setArtistId(doc.getArtistId());
            dto.setGenres(doc.getGenres());
            dto.setLabels(doc.getLabels());
            dto.setAlbum(doc.getAlbum());
            dto.setLength(doc.getLength());
            dto.setBoost(doc.getBoost());
            dto.setDescription(doc.getDescription());
            dto.setExpiresAt(doc.getExpiresAt());
            if (doc.getScheduler() != null) {
                Scheduler scheduler = doc.getScheduler();
                ScheduleDTO scheduleDTO = new ScheduleDTO();
                scheduleDTO.setEnabled(scheduler.isEnabled());
                if (scheduler.getTasks() != null && !scheduler.getTasks().isEmpty()) {
                    List<TaskDTO> taskDTOs = scheduler.getTasks().stream().map(task -> {
                        TaskDTO taskDTO = new TaskDTO();
                        taskDTO.setId(task.getId());
                        taskDTO.setTriggerType(task.getTriggerType());
                        if (task.getTriggerType() == TriggerType.ONCE && task.getOnceTrigger() != null) {
                            OnceTriggerDTO once = new OnceTriggerDTO();
                            once.setStartTime(task.getOnceTrigger().getStartTime());
                            once.setDuration(task.getOnceTrigger().getDuration());
                            once.setWeekdays(task.getOnceTrigger().getWeekdays());
                            taskDTO.setOnceTrigger(once);
                        }
                        if (task.getTriggerType() == TriggerType.PERIODIC && task.getPeriodicTrigger() != null) {
                            PeriodicTriggerDTO periodic = new PeriodicTriggerDTO();
                            PeriodicTrigger trigger = task.getPeriodicTrigger();
                            periodic.setStartTime(trigger.getStartTime());
                            periodic.setEndTime(trigger.getEndTime());
                            periodic.setWeekdays(trigger.getWeekdays());
                            periodic.setInterval(trigger.getInterval());
                            taskDTO.setPeriodicTrigger(periodic);
                        }
                        return taskDTO;
                    }).collect(Collectors.toList());
                    scheduleDTO.setTasks(taskDTOs);
                }
                dto.setSchedule(scheduleDTO);
            }
            dto.setUploadedFiles(files);
            dto.setRepresentedInBrands(representedInBrands);
            if (shares != null) {
                dto.setSharedWith(shares);
                dto.setShared(!shares.isEmpty());
            }
            return dto;
        });
    }

    private SoundFragment buildEntity(SoundFragmentDTO dto) {
        SoundFragment doc = new SoundFragment();
        doc.setStatus(dto.getStatus());
        doc.setType(dto.getType());
        doc.setTitle(dto.getTitle());
        doc.setArtist(dto.getArtist());
        doc.setArtistId(dto.getArtistId());
        doc.setGenres(dto.getGenres());
        doc.setLabels(dto.getLabels());
        doc.setAlbum(dto.getAlbum());
        doc.setLength(dto.getLength());
        doc.setBoost(dto.getBoost());
        doc.setDescription(dto.getDescription());
        doc.setExpiresAt(dto.getExpiresAt());
        doc.setSlugName(WebHelper.generateSlug(dto.getTitle(), dto.getArtist()));
        if (dto.getSchedule() != null) {
            ScheduleDTO scheduleDTO = dto.getSchedule();
            Scheduler scheduler = new Scheduler();
            scheduler.setEnabled(scheduleDTO.isEnabled());
            if (scheduleDTO.getTasks() != null && !scheduleDTO.getTasks().isEmpty()) {
                List<Task> tasks = scheduleDTO.getTasks().stream().map(taskDTO -> {
                    Task task = new Task();
                    task.setId(taskDTO.getId() != null ? taskDTO.getId() : java.util.UUID.randomUUID());
                    task.setTriggerType(taskDTO.getTriggerType());
                    if (taskDTO.getTriggerType() == TriggerType.ONCE && taskDTO.getOnceTrigger() != null) {
                        OnceTrigger once = new OnceTrigger();
                        once.setStartTime(taskDTO.getOnceTrigger().getStartTime());
                        once.setDuration(taskDTO.getOnceTrigger().getDuration());
                        once.setWeekdays(taskDTO.getOnceTrigger().getWeekdays());
                        task.setOnceTrigger(once);
                    }
                    if (taskDTO.getTriggerType() == TriggerType.PERIODIC && taskDTO.getPeriodicTrigger() != null) {
                        PeriodicTrigger periodic = new PeriodicTrigger();
                        periodic.setStartTime(taskDTO.getPeriodicTrigger().getStartTime());
                        periodic.setEndTime(taskDTO.getPeriodicTrigger().getEndTime());
                        periodic.setInterval(taskDTO.getPeriodicTrigger().getInterval());
                        periodic.setWeekdays(taskDTO.getPeriodicTrigger().getWeekdays());
                        task.setPeriodicTrigger(periodic);
                    }
                    return task;
                }).collect(Collectors.toList());
                scheduler.setTasks(tasks);
            }
            doc.setScheduler(scheduler);
        }
        return doc;
    }

    private Uni<List<RlsActionDTO>> buildRlsActionsWithCoOwners(List<UUID> brandIds, List<RlsActionDTO> existing) {
        if (brandIds == null || brandIds.isEmpty()) {
            return Uni.createFrom().item(existing != null ? existing : List.of());
        }
        List<Uni<Brand>> brandUnis = brandIds.stream()
                .map(brandId -> brandService.getById(brandId, SuperUser.build())
                        .onFailure().recoverWithNull())
                .collect(Collectors.toList());
        return Uni.join().all(brandUnis).andFailFast()
                .map(brands -> {
                    Set<Long> coOwnerIds = new HashSet<>();
                    for (Brand brand : brands) {
                        if (brand == null) continue;
                        Owner owner = brand.getOwner();
                        if (owner == null) continue;
                        if (owner.getUserId() != null) coOwnerIds.add(owner.getUserId());
                        if (owner.getCoOwners() != null) {
                            for (Owner co : owner.getCoOwners()) {
                                if (co.getUserId() != null) coOwnerIds.add(co.getUserId());
                            }
                        }
                    }
                    if (coOwnerIds.isEmpty()) {
                        return existing != null ? existing : List.<RlsActionDTO>of();
                    }
                    List<RlsActionDTO> combined = new ArrayList<>(existing != null ? existing : List.of());
                    for (Long userId : coOwnerIds) {
                        RlsActionDTO grant = new RlsActionDTO();
                        grant.setAction(RlsActionType.GRANT);
                        grant.setUserId(userId);
                        grant.setCanEdit(true);
                        grant.setCanDelete(true);
                        combined.add(grant);
                    }
                    return combined;
                });
    }

    public Uni<Integer> delete(String id, IUser user) {
        assert repository != null;
        return repository.delete(UUID.fromString(id), user);
    }

    public Uni<Integer> archive(String id, IUser user) {
        assert repository != null;
        return repository.archive(UUID.fromString(id), user);
    }

    public Uni<Integer> revokeMyAccess(UUID id, IUser user) {
        assert repository != null;
        return repository.revokeMyAccess(id, user);
    }

    public Uni<List<DocumentAccessDTO>> getDocumentAccess(UUID documentId, IUser user) {
        assert repository != null;
        return repository.getDocumentAccessInfo(documentId, user)
                .onItem().transform(accessInfoList ->
                        accessInfoList.stream()
                                .map(this::mapToDocumentAccessDTO)
                                .collect(Collectors.toList())
                );
    }

    public Uni<UUID> resolveBrandSlug(String brandSlug) {
        if (brandSlug == null || brandSlug.trim().isEmpty()) {
            return Uni.createFrom().nullItem();
        }
        
        assert brandService != null;
        return brandService.getBySlugName(brandSlug)
                .map(station -> station != null ? station.getId() : null)
                .onFailure().recoverWithItem(err -> {
                    LOGGER.warn("Failed to resolve brandSlug: %s", brandSlug, err);
                    return null;
                });
    }

    public Uni<SoundFragment> createFromBulkUpload(UploadFileDTO uploadFile, UUID brandId, IUser user) {
        if (uploadFile.getFullPath() == null) {
            return Uni.createFrom().failure(new IllegalArgumentException("Upload file has no fullPath"));
        }

        AudioMetadataDTO metadata = uploadFile.getMetadata();
        String fallbackTitleArtist = baseNameWithoutExtension(uploadFile.getName());

        SoundFragment fragment = new SoundFragment();
        fragment.setSource(SourceType.USER_UPLOAD);
        fragment.setStatus(1);
        fragment.setType(PlaylistItemType.SONG);
        if (metadata != null) {
            fragment.setTitle(metadata.getTitle() != null ? metadata.getTitle() : uploadFile.getName());
            fragment.setArtist(metadata.getArtist() != null ? metadata.getArtist() : "Unknown Artist");
            fragment.setAlbum(metadata.getAlbum());
            fragment.setLength(metadata.getLength());
        } else {
            LOGGER.infof("Bulk upload: no audio metadata; using file base name for title and artist: %s", fallbackTitleArtist);
            fragment.setTitle(fallbackTitleArtist);
            fragment.setArtist(fallbackTitleArtist);
            fragment.setAlbum(null);
            fragment.setLength(null);
        }
        fragment.setSlugName(WebHelper.generateSlug(fragment.getArtist(), fragment.getTitle()));

        FileMetadata fileMetadata = new FileMetadata();
        fileMetadata.setFilePath(Paths.get(uploadFile.getFullPath()));
        fragment.setFileMetadataList(List.of(fileMetadata));
        List<UUID> brandIds = brandId !=null ? List.of(brandId) : List.of();

        assert refService != null;
        String genreIdentifier = metadata != null && metadata.getGenre() != null ? metadata.getGenre() : "other";
        List<UUID> checkBrands = brandId != null ? List.of(brandId) : List.of();
        return checkBrandSongLimits(checkBrands, user)
                .chain(() -> genreService.getByFuzzyIdentifier(genreIdentifier)
                        .chain(genres -> {
                            List<UUID> genreIds = genres.stream().map(DataEntity::getId).collect(Collectors.toList());
                            fragment.setGenres(genreIds);
                            assert repository != null;
                            return repository.insert(fragment, brandIds, Collections.emptyList(), user);
                        })
                        .invoke(insertedFragment -> triggerOpusEncoding(insertedFragment, brandId, Paths.get(uploadFile.getFullPath()))));
    }

    private void triggerOpusEncodingIfMissing(SoundFragment fragment, UUID brandId, List<FileMetadata> newlyUploadedFiles) {
        if (fragment.getFileMetadataList() == null) return;
        boolean hasOpus = fragment.getFileMetadataList().stream()
                .anyMatch(m -> FileType.OPUS_ENCODED_SOUND_FRAGMENT.equals(m.getFileType()));
        if (hasOpus) return;

        // prefer local file from this upload
        Optional<Path> localPath = newlyUploadedFiles.stream()
                .map(FileMetadata::getFilePath)
                .filter(p -> p != null && Files.exists(p))
                .findFirst();

        if (localPath.isPresent()) {
            triggerOpusEncoding(fragment, brandId, localPath.get());
            return;
        }

        // no new local file — download original from Hetzner then encode
        FileMetadata original = fragment.getFileMetadataList().stream()
                .filter(m -> m.getFileType() == null || FileType.SOUND_FRAGMENT.equals(m.getFileType()))
                .findFirst().orElse(null);
        if (original == null || original.getFileKey() == null) {
            LOGGER.warnf("Opus encoding skipped on update: no original file found for fragment %s", fragment.getId());
            return;
        }

        LOGGER.infof("Opus encoding on update: downloading original from Hetzner key=%s for fragment %s",
                original.getFileKey(), fragment.getId());

        fileStorage.getFileStream(original.getFileKey())
                .chain(fileMeta -> fileMeta.materializeFileStream(uploadDir))
                .chain(tempPath -> {
                    triggerOpusEncoding(fragment, brandId, tempPath);
                    return Uni.createFrom().voidItem();
                })
                .subscribe().with(
                        ignored -> {},
                        err -> LOGGER.errorf(err, "Failed to download original for opus encoding, fragment %s", fragment.getId())
                );
    }

    private void triggerOpusEncoding(SoundFragment fragment, UUID brandId, Path localFilePath) {
        LOGGER.infof("Opus encoding triggered: fragment=%s, localFile=%s, exists=%s",
                fragment.getId(), localFilePath, Files.exists(localFilePath));
        if (opusEncodingService == null || fileStorage == null || repository == null) {
            LOGGER.warnf("Opus encoding skipped: services not initialized (opus=%s, storage=%s, repo=%s)",
                    opusEncodingService, fileStorage, repository);
            return;
        }
        if (!Files.exists(localFilePath)) {
            LOGGER.warnf("Opus encoding skipped: local file not found at %s", localFilePath);
            return;
        }
        List<FileMetadata> files = fragment.getFileMetadataList();
        if (files == null || files.isEmpty() || files.get(0).getFileKey() == null) {
            LOGGER.warnf("Opus encoding skipped: no file key on fragment %s", fragment.getId());
            return;
        }
        String originalKey = files.get(0).getFileKey();
        LOGGER.infof("Opus encoding: originalKey=%s, brandId=%s", originalKey, brandId);

        Uni<Long> bitRateUni = brandId != null
                ? brandService.getById(brandId, SuperUser.build())
                    .invoke(brand -> LOGGER.infof("Opus encoding: brand bitRate=%d bps -> %d kbps", brand.getBitRate(), brand.getBitRate() / 1000))
                    .map(brand -> brand.getBitRate() > 0 ? brand.getBitRate() / 1000 : 128L)
                : Uni.createFrom().item(128L);

        bitRateUni
                .chain(bitRate -> {
                    LOGGER.infof("Opus encoding: starting ffmpeg encode at %dkbps for %s", bitRate, localFilePath);
                    return Uni.createFrom().item(() -> {
                        try {
                            return opusEncodingService.encode(localFilePath, bitRate);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
                })
                .chain(encodedFile -> {
                    String encodedKey = originalKey + ".opus";
                    LOGGER.infof("Opus encoding: ffmpeg done, uploading to Hetzner key=%s, localFile=%s", encodedKey, encodedFile);
                    return fileStorage.uploadFile(encodedKey, encodedFile.toString(), "audio/ogg")
                            .chain(ignored -> {
                                try { Files.deleteIfExists(encodedFile); } catch (Exception ignored2) {}
                                LOGGER.infof("Opus encoding: upload done, inserting _files record for fragment %s", fragment.getId());
                                FileMetadata encodedMeta = new FileMetadata();
                                encodedMeta.setFileKey(encodedKey);
                                encodedMeta.setFileType(FileType.OPUS_ENCODED_SOUND_FRAGMENT);
                                encodedMeta.setMimeType("audio/ogg");
                                FileMetadata orig = files.get(0);
                                String origName = orig.getFileOriginalName() != null ? orig.getFileOriginalName() : localFilePath.getFileName().toString();
                                encodedMeta.setFileOriginalName(origName + ".opus");
                                encodedMeta.setSlugName(files.get(0).getSlugName() + "-opus");
                                return repository.insertEncodedFile(fragment.getId(), encodedMeta);
                            });
                })
                .subscribe().with(
                        ignored -> LOGGER.infof("Opus encoding complete for fragment %s", fragment.getId()),
                        err -> LOGGER.errorf(err, "Opus encoding failed for fragment %s", fragment.getId())
                );
    }

    private static String baseNameWithoutExtension(String fileName) {
        if (fileName == null || fileName.isBlank()) {
            return "Untitled";
        }
        String name = fileName.trim();
        int dot = name.lastIndexOf('.');
        if (dot > 0) {
            return name.substring(0, dot);
        }
        return name;
    }

}
