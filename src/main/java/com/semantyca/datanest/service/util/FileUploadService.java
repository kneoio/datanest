package com.semantyca.datanest.service.util;

import com.semantyca.core.model.user.IUser;
import com.semantyca.core.util.FileSecurityUtils;
import com.semantyca.datanest.config.DatanestConfig;
import com.semantyca.datanest.dto.AudioMetadataDTO;
import com.semantyca.datanest.dto.PublicSubmissionMetaDTO;
import com.semantyca.datanest.dto.UploadFileDTO;
import com.semantyca.datanest.service.manipulation.AudioMetadataService;
import com.semantyca.datanest.service.soundfragment.SoundFragmentService;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.vertx.core.Vertx;
import io.vertx.ext.web.RoutingContext;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@ApplicationScoped
public class FileUploadService {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileUploadService.class);
    private static final long MAX_FILE_SIZE_BYTES = 200 * 1024 * 1024; //200 mb
    private static final String BULK_FOLDER_NAME = "bulk";
    // Must match PublicSongSubmissionController.CONTROLLER_KEY — anonymous public submissions
    // need station-owner approval, unlike an authenticated station owner's own bulk upload.
    private static final String PUBLIC_SUBMISSIONS_CONTROLLER_KEY = "public-submissions";
    private static final long BULK_SSE_POLL_MS = 500L;
    private static final long BULK_SSE_KEEPALIVE_MS = 30_000L;
    private final String uploadDirectory;
    private final AudioMetadataService audioMetadataService;
    private final SoundFragmentService soundFragmentService;
    private final Vertx vertx;
    public final ConcurrentHashMap<String, UploadFileDTO> uploadProgressMap = new ConcurrentHashMap<>();
    public final ConcurrentHashMap<String, ConcurrentHashMap<String, UploadFileDTO>> bulkUploadProgressMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, List<UUID>> batchBrandIdMap = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ChunkAssemblyState> chunkStateMap = new ConcurrentHashMap<>();

    private static final class ChunkAssemblyState {
        final int totalChunks;
        final AtomicInteger receivedCount = new AtomicInteger(0);
        final String originalFileName;
        final String safeFileName;
        final String batchId;
        final String entityId; // null = bulk mode; UUID string = single-entity mode

        ChunkAssemblyState(int totalChunks, String originalFileName, String safeFileName, String batchId, String entityId) {
            this.totalChunks = totalChunks;
            this.originalFileName = originalFileName;
            this.safeFileName = safeFileName;
            this.batchId = batchId;
            this.entityId = entityId;
        }
    }

    @Inject
    public FileUploadService(DatanestConfig config, AudioMetadataService audioMetadataService,
                             SoundFragmentService soundFragmentService, Vertx vertx) {
        this.uploadDirectory = config.getPathUploads();
        this.audioMetadataService = audioMetadataService;
        this.soundFragmentService = soundFragmentService;
        this.vertx = vertx;
    }

    public void validateUploadMeta(String originalFileName, String contentType) {
        if (!isValidAudioFile(originalFileName, contentType)) {
            throw new IllegalArgumentException("Unsupported file type. Only audio files are allowed: " +
                    String.join(", ", SUPPORTED_AUDIO_EXTENSIONS));
        }
    }

    private Uni<Void> resolveBrandSlugIfNeeded(String batchId, String brandSlug) {
        return resolveBrandSlugsIfNeeded(batchId, brandSlug == null || brandSlug.trim().isEmpty() ? List.of() : List.of(brandSlug));
    }

    // Resolves each station slug to a brand UUID once per batch (cached, so later chunks of the
    // same upload don't need to resend/re-resolve them). A submission may target multiple
    // stations — each becomes its own independent PENDING share on the same SoundFragment, see
    // SharedSoundFragmentService.shareContribution and SHARING_WORKFLOW.md §3.
    private Uni<Void> resolveBrandSlugsIfNeeded(String batchId, List<String> brandSlugs) {
        if (brandSlugs.isEmpty()) {
            return Uni.createFrom().voidItem();
        }

        // Check if already resolved for this batch
        if (batchBrandIdMap.containsKey(batchId)) {
            return Uni.createFrom().voidItem();
        }

        return Uni.join().all(
                        brandSlugs.stream().map(soundFragmentService::resolveBrandSlug).collect(Collectors.toList()))
                .andFailFast()
                .map(brandIds -> {
                    List<UUID> resolved = brandIds.stream().filter(java.util.Objects::nonNull).collect(Collectors.toList());
                    if (!resolved.isEmpty()) {
                        batchBrandIdMap.put(batchId, resolved);
                    }
                    return null;
                });
    }

    public Uni<UploadFileDTO> processDirectBulkStreamAsync(RoutingContext rc, String batchId, String fileId, String brandSlug, String controllerKey, IUser user) {
        return resolveBrandSlugIfNeeded(batchId, brandSlug)
                .chain(() -> processFileUpload(rc, batchId, fileId, controllerKey, user, null));
    }

    public Uni<UploadFileDTO> processDirectBulkStreamAsync(RoutingContext rc, String batchId, String fileId, List<String> brandSlugs, String controllerKey, IUser user, PublicSubmissionMetaDTO meta) {
        // Resolve all target stations once for the batch before processing
        return resolveBrandSlugsIfNeeded(batchId, brandSlugs)
                .chain(() -> processFileUpload(rc, batchId, fileId, controllerKey, user, meta));
    }

    private Uni<UploadFileDTO> processFileUpload(RoutingContext rc, String batchId, String fileId, String controllerKey, IUser user, PublicSubmissionMetaDTO meta) {
        return Uni.createFrom().<UploadFileDTO>emitter(emitter -> {
            try {
                rc.request().setExpectMultipart(true);

                rc.request().uploadHandler(upload -> {
                    try {
                        String originalFileName = upload.filename();
                        validateUploadMeta(originalFileName, upload.contentType());
                        String safeFileName = sanitizeAndValidateFilename(originalFileName, user);
                        Path destination = setupDirectoriesAndPath(controllerKey, BULK_FOLDER_NAME, user, safeFileName);
                        Path tempInFinalDir = destination.getParent().resolve(".tmp_" + batchId + "_" + safeFileName);
                        upload.streamToFileSystem(tempInFinalDir.toString());

                        upload.endHandler(v -> {
                            try {
                                LOGGER.debug("Upload end handler fired: batchId={}, fileId={}, tempFile={}, exists={}",
                                        batchId, fileId, tempInFinalDir, Files.exists(tempInFinalDir));
                                if (!Files.exists(tempInFinalDir)) {
                                    LOGGER.error("Temp file missing at end handler: batchId={}, fileId={}, path={}",
                                            batchId, fileId, tempInFinalDir);
                                    emitter.fail(new java.nio.file.NoSuchFileException(tempInFinalDir.toString(),
                                            null, "Temp file was removed before upload completed (likely cleaned up by a concurrent failed upload)"));
                                    return;
                                }
                                if (Files.size(tempInFinalDir) > MAX_FILE_SIZE_BYTES) {
                                    Files.deleteIfExists(tempInFinalDir);
                                    emitter.fail(new IllegalArgumentException(String.format("File too large. Maximum size is %d MB",
                                            MAX_FILE_SIZE_BYTES / 1024 / 1024)));
                                    return;
                                }
                                Files.move(tempInFinalDir, destination, StandardCopyOption.REPLACE_EXISTING);
                                String fileUrl = generateFileUrl(BULK_FOLDER_NAME, safeFileName);

                                // Get or create batch map once
                                ConcurrentHashMap<String, UploadFileDTO> batchMap = bulkUploadProgressMap.computeIfAbsent(batchId, k -> new ConcurrentHashMap<>());

                                // Return immediately with processing status
                                UploadFileDTO dto = UploadFileDTO.builder()
                                        .id(fileId)
                                        .status("processing")
                                        .percentage(100)
                                        .batchId(batchId)
                                        .name(safeFileName)
                                        .url(fileUrl)
                                        .build();
                                
                                batchMap.put(fileId, dto);
                                emitter.complete(dto);

                                // Extract metadata async in background
                                Uni.createFrom().item(() -> {
                                    AudioMetadataDTO metadata = extractMetadata(destination, originalFileName, fileId);
                                    UploadFileDTO metadataDto = UploadFileDTO.builder()
                                            .id(fileId)
                                            .status("metadata_extracted")
                                            .percentage(100)
                                            .batchId(batchId)
                                            .name(safeFileName)
                                            .url(fileUrl)
                                            .fullPath(destination.toString())
                                            .metadata(metadata)
                                            .build();
                                    
                                    batchMap.put(fileId, metadataDto);
                                    return metadataDto;
                                }).runSubscriptionOn(Infrastructure.getDefaultExecutor())
                                .chain(metadataDto -> {
                                    // Create SoundFragment entity
                                    UploadFileDTO creatingDto = UploadFileDTO.builder()
                                            .id(fileId)
                                            .status("creating_entity")
                                            .percentage(100)
                                            .batchId(batchId)
                                            .name(safeFileName)
                                            .url(fileUrl)
                                            .fullPath(metadataDto.getFullPath())
                                            .metadata(metadataDto.getMetadata())
                                            .build();
                                    batchMap.put(fileId, creatingDto);
                                    
                                    // Get cached target brand ids for this batch
                                    List<UUID> targetBrandIds = batchBrandIdMap.getOrDefault(batchId, List.of());

                                    return soundFragmentService.createFromBulkUpload(metadataDto, targetBrandIds, user,
                                                    PUBLIC_SUBMISSIONS_CONTROLLER_KEY.equals(controllerKey), meta)
                                            .map(fragment -> {
                                                UploadFileDTO finalDto = UploadFileDTO.builder()
                                                        .id(fileId)
                                                        .status("finished")
                                                        .percentage(100)
                                                        .batchId(batchId)
                                                        .name(safeFileName)
                                                        .url(fileUrl)
                                                        .metadata(metadataDto.getMetadata())
                                                        .build();
                                                batchMap.put(fileId, finalDto);
                                                return finalDto;
                                            });
                                })
                                .subscribe().with(
                                    result -> LOGGER.info("Bulk upload completed for fileId: {}", fileId),
                                    err -> {
                                        LOGGER.error("Bulk upload failed for fileId: {}", fileId, err);
                                        UploadFileDTO errorDto = UploadFileDTO.builder()
                                                .id(fileId)
                                                .status("error")
                                                .percentage(100)
                                                .batchId(batchId)
                                                .name(safeFileName)
                                                .url(fileUrl)
                                                .errorMessage("Upload failed: " + err.getMessage())
                                                .build();
                                        
                                        batchMap.put(fileId, errorDto);
                                    }
                                );

                            } catch (Exception e) {
                                try {
                                    Files.deleteIfExists(tempInFinalDir);
                                    Files.deleteIfExists(destination);
                                } catch (IOException ignored) {}
                                emitter.fail(e);
                            }
                        });

                        upload.exceptionHandler(err -> {
                            LOGGER.warn("Upload exception handler fired: batchId={}, fileId={}, error={}",
                                    batchId, fileId, err.getMessage());
                            try {
                                Files.deleteIfExists(tempInFinalDir);
                            } catch (IOException ignored) {}
                            emitter.fail(err);
                        });

                    } catch (Exception e) {
                        emitter.fail(e);
                    }
                });
            } catch (Exception e) {
                emitter.fail(e);
            }
        }).emitOn(Infrastructure.getDefaultExecutor());
    }

    public Uni<UploadFileDTO> processChunkUpload(RoutingContext rc, String batchId, String fileId,
            int chunkIndex, int totalChunks, String originalFileName,
            String entityId, String brandSlug, String controllerKey, IUser user) {
        validateFileId(fileId);
        return resolveBrandSlugIfNeeded(batchId, brandSlug)
                .chain(() -> writeChunk(rc, batchId, fileId, chunkIndex, totalChunks, originalFileName, entityId, controllerKey, user, null));
    }

    public Uni<UploadFileDTO> processChunkUpload(RoutingContext rc, String batchId, String fileId,
            int chunkIndex, int totalChunks, String originalFileName,
            String entityId, List<String> brandSlugs, String controllerKey, IUser user, PublicSubmissionMetaDTO meta) {
        validateFileId(fileId);
        return resolveBrandSlugsIfNeeded(batchId, brandSlugs)
                .chain(() -> writeChunk(rc, batchId, fileId, chunkIndex, totalChunks, originalFileName, entityId, controllerKey, user, meta));
    }

    private Uni<UploadFileDTO> writeChunk(RoutingContext rc, String batchId, String fileId,
            int chunkIndex, int totalChunks, String originalFileName, String entityId,
            String controllerKey, IUser user, PublicSubmissionMetaDTO meta) {
        return Uni.createFrom().<UploadFileDTO>emitter(emitter -> {
            try {
                rc.request().setExpectMultipart(true);
                rc.request().uploadHandler(upload -> {
                    try {
                        validateUploadMeta(originalFileName, null);
                        String safeFileName = sanitizeAndValidateFilename(originalFileName, user);

                        ChunkAssemblyState state = chunkStateMap.computeIfAbsent(fileId,
                                k -> new ChunkAssemblyState(totalChunks, originalFileName, safeFileName, batchId, entityId));

                        Path chunkFile = buildChunkPath(fileId, chunkIndex, controllerKey, user.getUserName());
                        upload.streamToFileSystem(chunkFile.toString());

                        upload.endHandler(v -> {
                            try {
                                if (!Files.exists(chunkFile)) {
                                    LOGGER.error("Chunk file missing: fileId={}, chunk={}/{}", fileId, chunkIndex, totalChunks);
                                    emitter.fail(new java.nio.file.NoSuchFileException(chunkFile.toString()));
                                    return;
                                }

                                int received = state.receivedCount.incrementAndGet();
                                int percentage = received * 100 / totalChunks;
                                LOGGER.debug("Chunk {}/{} received: fileId={}", received, totalChunks, fileId);

                                if (received < totalChunks) {
                                    if (state.entityId == null) {
                                        bulkUploadProgressMap.computeIfAbsent(batchId, k -> new ConcurrentHashMap<>())
                                                .put(fileId, UploadFileDTO.builder()
                                                        .id(fileId).status("uploading").percentage(percentage)
                                                        .batchId(batchId).name(safeFileName).build());
                                    }
                                    emitter.complete(UploadFileDTO.builder()
                                            .id(fileId).status("uploading").percentage(percentage)
                                            .batchId(batchId).name(safeFileName).build());
                                } else if (state.entityId != null) {
                                    // Single-entity: assemble + extract metadata on worker thread, return full DTO
                                    Uni.createFrom().<UploadFileDTO>item(() -> {
                                        try {
                                            Path destination = assembleChunks(fileId, state, controllerKey, user);
                                            String fileUrl = generateFileUrl(state.entityId, state.safeFileName);
                                            AudioMetadataDTO metadata = extractMetadata(destination, state.originalFileName, fileId);
                                            return UploadFileDTO.builder()
                                                    .id(fileId).status("finished").percentage(100)
                                                    .batchId(batchId).name(state.safeFileName).url(fileUrl)
                                                    .metadata(metadata).build();
                                        } catch (Exception e) {
                                            throw new RuntimeException(e);
                                        }
                                    }).runSubscriptionOn(Infrastructure.getDefaultExecutor())
                                    .subscribe().with(emitter::complete, emitter::fail);
                                } else {
                                    // Bulk: respond immediately, assemble + create entity in background
                                    String fileUrl = generateFileUrl(BULK_FOLDER_NAME, safeFileName);
                                    ConcurrentHashMap<String, UploadFileDTO> batchMap =
                                            bulkUploadProgressMap.computeIfAbsent(batchId, k -> new ConcurrentHashMap<>());
                                    UploadFileDTO processingDto = UploadFileDTO.builder()
                                            .id(fileId).status("processing").percentage(100)
                                            .batchId(batchId).name(safeFileName).url(fileUrl).build();
                                    batchMap.put(fileId, processingDto);
                                    emitter.complete(processingDto);
                                    assembleAndProcess(batchId, fileId, state, controllerKey, user, batchMap, meta);
                                }
                            } catch (Exception e) {
                                try { Files.deleteIfExists(chunkFile); } catch (IOException ignored) {}
                                emitter.fail(e);
                            }
                        });

                        upload.exceptionHandler(err -> {
                            LOGGER.warn("Chunk upload exception: fileId={}, chunk={}/{}, error={}",
                                    fileId, chunkIndex, totalChunks, err.getMessage());
                            try { Files.deleteIfExists(chunkFile); } catch (IOException ignored) {}
                            emitter.fail(err);
                        });

                    } catch (Exception e) {
                        emitter.fail(e);
                    }
                });
            } catch (Exception e) {
                emitter.fail(e);
            }
        }).emitOn(Infrastructure.getDefaultExecutor());
    }

    private void assembleAndProcess(String batchId, String fileId, ChunkAssemblyState state,
            String controllerKey, IUser user,
            ConcurrentHashMap<String, UploadFileDTO> batchMap, PublicSubmissionMetaDTO meta) {
        Uni.createFrom().<UploadFileDTO>item(() -> {
            try {
                Path destination = assembleChunks(fileId, state, controllerKey, user);
                String fileUrl = generateFileUrl(BULK_FOLDER_NAME, state.safeFileName);
                AudioMetadataDTO metadata = extractMetadata(destination, state.originalFileName, fileId);
                UploadFileDTO metadataDto = UploadFileDTO.builder()
                        .id(fileId).status("metadata_extracted").percentage(100)
                        .batchId(batchId).name(state.safeFileName).url(fileUrl)
                        .fullPath(destination.toString()).metadata(metadata).build();
                batchMap.put(fileId, metadataDto);
                return metadataDto;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }).runSubscriptionOn(Infrastructure.getDefaultExecutor())
        .chain(metadataDto -> {
            String fileUrl = generateFileUrl(BULK_FOLDER_NAME, state.safeFileName);
            UploadFileDTO creatingDto = UploadFileDTO.builder()
                    .id(fileId).status("creating_entity").percentage(100)
                    .batchId(batchId).name(state.safeFileName).url(fileUrl)
                    .fullPath(metadataDto.getFullPath()).metadata(metadataDto.getMetadata()).build();
            batchMap.put(fileId, creatingDto);
            List<UUID> targetBrandIds = batchBrandIdMap.getOrDefault(batchId, List.of());
            return soundFragmentService.createFromBulkUpload(metadataDto, targetBrandIds, user,
                            PUBLIC_SUBMISSIONS_CONTROLLER_KEY.equals(controllerKey), meta)
                    .map(fragment -> {
                        UploadFileDTO finalDto = UploadFileDTO.builder()
                                .id(fileId).status("finished").percentage(100)
                                .batchId(batchId).name(state.safeFileName).url(fileUrl)
                                .metadata(metadataDto.getMetadata()).build();
                        batchMap.put(fileId, finalDto);
                        return finalDto;
                    });
        })
        .subscribe().with(
                result -> LOGGER.info("Chunked upload completed: fileId={}", fileId),
                err -> {
                    LOGGER.error("Chunked upload failed: fileId={}", fileId, err);
                    String fileUrl = generateFileUrl(BULK_FOLDER_NAME, state.safeFileName);
                    batchMap.put(fileId, UploadFileDTO.builder()
                            .id(fileId).status("error").percentage(100)
                            .batchId(batchId).name(state.safeFileName).url(fileUrl)
                            .errorMessage("Assembly failed: " + err.getMessage()).build());
                    cleanupChunkDir(fileId, controllerKey, user.getUserName());
                }
        );
    }

    private Path assembleChunks(String fileId, ChunkAssemblyState state, String controllerKey, IUser user) throws Exception {
        String folder = state.entityId != null ? state.entityId : BULK_FOLDER_NAME;
        Path destination = setupDirectoriesAndPath(controllerKey, folder, user, state.safeFileName);
        Path tempAssembled = destination.getParent().resolve(".tmp_assembled_" + fileId + "_" + state.safeFileName);
        try (OutputStream out = Files.newOutputStream(tempAssembled)) {
            for (int i = 0; i < state.totalChunks; i++) {
                Path chunkFile = buildChunkPath(fileId, i, controllerKey, user.getUserName());
                Files.copy(chunkFile, out);
                Files.delete(chunkFile);
            }
        }
        Files.move(tempAssembled, destination, StandardCopyOption.REPLACE_EXISTING);
        cleanupChunkDir(fileId, controllerKey, user.getUserName());
        chunkStateMap.remove(fileId);
        return destination;
    }

    private Path buildChunkPath(String fileId, int chunkIndex, String controllerKey, String userName) {
        try {
            Path dir = Files.createDirectories(
                    Paths.get(uploadDirectory, controllerKey, userName, BULK_FOLDER_NAME, ".chunks_" + fileId));
            return dir.resolve("chunk_" + chunkIndex);
        } catch (IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }

    private void cleanupChunkDir(String fileId, String controllerKey, String userName) {
        Path dir = Paths.get(uploadDirectory, controllerKey, userName, BULK_FOLDER_NAME, ".chunks_" + fileId);
        if (!Files.exists(dir)) return;
        try (java.util.stream.Stream<Path> files = Files.list(dir)) {
            files.forEach(p -> { try { Files.deleteIfExists(p); } catch (IOException ignored) {} });
        } catch (IOException ignored) {}
        try { Files.deleteIfExists(dir); } catch (IOException ignored) {}
    }

    private static void validateFileId(String fileId) {
        if (fileId == null || !fileId.matches("[a-zA-Z0-9_-]+")) {
            throw new IllegalArgumentException("Invalid fileId");
        }
    }

    public ConcurrentHashMap<String, UploadFileDTO> getBulkUploadProgress(String batchId) {
        ConcurrentHashMap<String, UploadFileDTO> files = bulkUploadProgressMap.get(batchId);
        return files != null ? files : new ConcurrentHashMap<>();
    }

    // Shared by SoundFragmentBulkUploadController (authenticated bulk upload) and
    // PublicSongSubmissionController (anonymous public submission) — both poll the same
    // bulkUploadProgressMap, keyed by batchId, so the SSE polling/keepalive logic only needs
    // to exist once.
    public void streamBulkProgress(RoutingContext rc, String batchId) {
        rc.response()
                .putHeader("Content-Type", "text/event-stream")
                .putHeader("Cache-Control", "no-cache")
                .setChunked(true);

        ConcurrentHashMap<String, UploadFileDTO> snapshot = getBulkUploadProgress(batchId);
        if (!snapshot.isEmpty()) {
            rc.response().write("data: " + io.vertx.core.json.Json.encode(snapshot) + "\n\n");
            if (allBulkFilesTerminal(snapshot)) {
                rc.response().end();
                return;
            }
        }

        final long[] timerIds = new long[2];
        timerIds[1] = vertx.setPeriodic(BULK_SSE_KEEPALIVE_MS, id -> rc.response().write(":\n\n"));
        timerIds[0] = vertx.setPeriodic(BULK_SSE_POLL_MS, id -> {
            ConcurrentHashMap<String, UploadFileDTO> files = getBulkUploadProgress(batchId);
            if (!files.isEmpty()) {
                rc.response().write("data: " + io.vertx.core.json.Json.encode(files) + "\n\n");
                if (allBulkFilesTerminal(files)) {
                    vertx.cancelTimer(timerIds[0]);
                    vertx.cancelTimer(timerIds[1]);
                    rc.response().end();
                }
            }
        });

        rc.request().connection().closeHandler(v -> {
            vertx.cancelTimer(timerIds[0]);
            vertx.cancelTimer(timerIds[1]);
        });
    }

    private static boolean allBulkFilesTerminal(Map<String, UploadFileDTO> files) {
        return !files.isEmpty()
                && files.values().stream()
                .allMatch(f -> "finished".equals(f.getStatus()) || "error".equals(f.getStatus()));
    }

    private String sanitizeAndValidateFilename(String originalFileName, IUser user) {
        try {
            return FileSecurityUtils.sanitizeFilename(originalFileName);
        } catch (SecurityException e) {
            LOGGER.warn("Unsafe filename rejected: {} from user: {}", originalFileName, user.getUserName());
            throw new IllegalArgumentException("Invalid filename: " + e.getMessage());
        }
    }

    private Path setupDirectoriesAndPath(String controllerKey, String entityId, IUser user, String safeFileName) throws Exception {
        if (!"temp".equals(entityId) && !"bulk".equals(entityId)) {
            try {
                UUID.fromString(entityId);
            } catch (IllegalArgumentException e) {
                LOGGER.warn("Invalid entity ID: {}, controller: {}", entityId, controllerKey);
                throw new IllegalArgumentException("Invalid entity ID");
            }
        }

        Path userDir = Files.createDirectories(Paths.get(uploadDirectory, controllerKey, user.getUserName()));
        Path entityDir = Files.createDirectories(userDir.resolve(entityId));
        Path destination = FileSecurityUtils.secureResolve(entityDir, safeFileName);
        Path expectedBase = Paths.get(uploadDirectory, controllerKey, user.getUserName(), entityId);

        if (!FileSecurityUtils.isPathWithinBase(expectedBase, destination)) {
            LOGGER.error("Security violation: Path traversal attempt with filename {}", safeFileName);
            throw new SecurityException("Invalid file path");
        }

        return destination;
    }

    private AudioMetadataDTO extractMetadata(Path destination, String originalFileName, String uploadId) {
        if (!isValidAudioFile(originalFileName, null)) {
            LOGGER.info("Skipping metadata extraction for non-audio file: {}", originalFileName);
            updateProgress(uploadId, 90, "extract_metadata", null, null, null, null);
            return null;
        }

        try {
            LOGGER.info("Extracting metadata for file: {}", destination);
            updateProgress(uploadId, 75, "extract_metadata", null, null, null, null);

            AudioMetadataDTO metadata = audioMetadataService.extractMetadataWithProgress(
                    destination.toString(),
                    (percentage) -> {
                        int overallProgress = 75 + (percentage * 15 / 100);
                        updateProgress(uploadId, Math.min(overallProgress, 90), "extract_metadata", null, null, null, null);
                    }
            );

            LOGGER.info("Metadata extraction completed for uploadId: {}", uploadId);
            return metadata;
        } catch (Exception e) {
            LOGGER.warn("Metadata extraction failed for uploadId: {}, error: {}", uploadId, e.getMessage());
            updateProgress(uploadId, 90, "extract_metadata", null, null, null, null);
            throw new RuntimeException("Not a valid/decodable audio file: " + originalFileName, e);
        }
    }

    private String generateFileUrl(String entityId, String safeFileName) {
        String entityIdSafe = entityId != null ? entityId : "temp";
        return String.format("/datanest/soundfragments/files/%s/%s", entityIdSafe, safeFileName);
    }

    private void updateProgress(String uploadId, Integer percentage, String status, String url, String fullPath, AudioMetadataDTO metadata, String errorMessage) {
        UploadFileDTO dto = uploadProgressMap.get(uploadId);
        if (dto == null) {
            UploadFileDTO init = UploadFileDTO.builder()
                    .status(status != null ? status : "uploading")
                    .percentage(percentage != null ? percentage : 0)
                    .batchId(uploadId)
                    .build();
            uploadProgressMap.put(uploadId, init);
            dto = init;
        }

        UploadFileDTO updatedDto = UploadFileDTO.builder()
                .id(dto.getId())
                .name(dto.getName())
                .status(status)
                .percentage(percentage != null ? percentage : dto.getPercentage())
                .url(url != null ? url : dto.getUrl())
                .batchId(dto.getBatchId())
                .type(dto.getType())
                .fullPath(fullPath != null ? fullPath : dto.getFullPath())
                .thumbnailUrl(dto.getThumbnailUrl())
                .metadata(metadata != null ? metadata : dto.getMetadata())
                .fileSize(dto.getFileSize())
                .errorMessage(errorMessage != null ? errorMessage : dto.getErrorMessage())
                .build();

        uploadProgressMap.put(uploadId, updatedDto);
    }

    private boolean isValidAudioFile(String filename, String contentType) {
        if (filename == null || filename.trim().isEmpty()) {
            return false;
        }

        String extension = getFileExtension(filename.toLowerCase());
        boolean validExtension = SUPPORTED_AUDIO_EXTENSIONS.contains(extension);

        boolean validMimeType = contentType != null &&
                SUPPORTED_AUDIO_MIME_TYPES.stream().anyMatch(contentType::startsWith);

        return validExtension || validMimeType;
    }

    private String getFileExtension(String filename) {
        int lastDot = filename.lastIndexOf('.');
        if (lastDot > 0 && lastDot < filename.length() - 1) {
            return filename.substring(lastDot + 1);
        }
        return "";
    }


    private static final Set<String> SUPPORTED_AUDIO_EXTENSIONS = Set.of(
            "mp3", "wav", "flac", "aac", "ogg", "m4a"
    );

    private static final Set<String> SUPPORTED_AUDIO_MIME_TYPES = Set.of(
            "audio/mpeg", "audio/wav", "audio/wave", "audio/x-wav",
            "audio/flac", "audio/x-flac", "audio/aac", "audio/ogg",
            "audio/mp4", "audio/x-m4a"
    );
}