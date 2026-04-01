package com.semantyca.datanest.service.maintenance;

import com.semantyca.datanest.config.DatanestConfig;
import io.quarkus.runtime.StartupEvent;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.smallrye.mutiny.subscription.Cancellable;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.core.file.FileSystem;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import lombok.Getter;
import org.jboss.logging.Logger;


import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

@ApplicationScoped
public class LocalFileCleanupService {
    private static final Logger LOGGER = Logger.getLogger(LocalFileCleanupService.class);
    private static final Duration TEMP_FILE_MAX_AGE = Duration.ofHours(2);
    private static final Duration ENTITY_FILE_MAX_AGE = Duration.ofDays(1);
    private static final Duration CLEANUP_INTERVAL = Duration.ofHours(1);
    private static final Duration INITIAL_DELAY = Duration.ofMinutes(10);

    private final FileSystem fileSystem;
    private final List<String> managedDirectories;
    private Cancellable cleanupSubscription;

    private final AtomicLong tempFilesDeleted = new AtomicLong(0);
    private final AtomicLong entityFilesDeleted = new AtomicLong(0);
    private final AtomicLong bytesFreed = new AtomicLong(0);
    private LocalDateTime lastCleanupTime;

    @Inject
    public LocalFileCleanupService(DatanestConfig config, Vertx vertx) {
        this.fileSystem = vertx.fileSystem();
        String baseUploadPath = config.getPathUploads();
        this.managedDirectories = List.of(
                baseUploadPath + "/sound-fragments-controller"
        );
    }

    void onStart(@Observes StartupEvent event) {
        LOGGER.infof("Starting Local File Cleanup Service for directories: %s", managedDirectories);
        startCleanupTask();
    }

    private void startCleanupTask() {
        cleanupSubscription = Multi.createFrom().ticks()
                .startingAfter(INITIAL_DELAY)
                .every(CLEANUP_INTERVAL)
                .onOverflow().drop()
                .onItem().invoke(this::performCleanup)
                .onFailure().invoke(error -> LOGGER.error("Local file cleanup error", error))
                .subscribe().with(
                        item -> {
                        },
                        failure -> LOGGER.error("Local file cleanup subscription failed", failure)
                );
    }

    public void stopCleanupTask() {
        if (cleanupSubscription != null) {
            cleanupSubscription.cancel();
        }
    }

    private void performCleanup(Long tick) {
        LOGGER.infof("Starting local file cleanup (tick: %s)", tick);

        long startTime = System.currentTimeMillis();
        long tempDeleted = 0;
        long entityDeleted = 0;
        long bytesFreedSession = 0;

        try {
            for (String directoryPath : managedDirectories) {
                Path uploadPath = Paths.get(directoryPath);
                if (!Files.exists(uploadPath)) {
                    LOGGER.debugf("Directory does not exist: %s", uploadPath);
                    continue;
                }

                CleanupResult result = cleanupDirectory(uploadPath).await().atMost(Duration.ofMinutes(5));
                tempDeleted += result.tempFilesDeleted;
                entityDeleted += result.entityFilesDeleted;
                bytesFreedSession += result.bytesFreed;
            }

            tempFilesDeleted.addAndGet(tempDeleted);
            entityFilesDeleted.addAndGet(entityDeleted);
            bytesFreed.addAndGet(bytesFreedSession);
            lastCleanupTime = LocalDateTime.now();

            long duration = System.currentTimeMillis() - startTime;
            double mbFreed = (double) bytesFreedSession / (1024 * 1024);

            LOGGER.infof("Local file cleanup completed in %sms. Temp files deleted: %s, Entity files deleted: %s, Space freed: %s MB",
                    duration, tempDeleted, entityDeleted, mbFreed);

        } catch (Exception e) {
            LOGGER.error("Error during local file cleanup", e);
        }
    }

    private Uni<CleanupResult> cleanupDirectory(Path baseDir) {
        return Uni.createFrom().item(() -> {
            CleanupResult result = new CleanupResult();

            try {
                if (isSpecialTempDirectory(baseDir)) {
                    // Handle audio-processing and playlist-processing directories
                    result.add(cleanupTempDirectory(baseDir));
                } else {
                    // Handle sound-fragments-controller structure with user directories
                    try (Stream<Path> userDirs = Files.list(baseDir)) {
                        for (Path userDir : userDirs.toArray(Path[]::new)) {
                            if (Files.isDirectory(userDir)) {
                                result.add(cleanupUserDirectory(userDir));
                            }
                        }
                    }
                }
            } catch (Exception e) {
                LOGGER.error("Failed to cleanup directory: %s", baseDir, e);
            }

            return result;
        }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
    }

    private boolean isSpecialTempDirectory(Path directory) {
        String dirName = directory.getFileName().toString();
        return "audio-processing".equals(dirName) || "playlist-processing".equals(dirName);
    }

    private CleanupResult cleanupUserDirectory(Path userDir) {
        CleanupResult result = new CleanupResult();

        try (Stream<Path> entityDirs = Files.list(userDir)) {
            for (Path entityDir : entityDirs.toArray(Path[]::new)) {
                if (Files.isDirectory(entityDir)) {
                    String dirName = entityDir.getFileName().toString();

                    if ("temp".equals(dirName)) {
                        result.add(cleanupTempDirectory(entityDir));
                    } else {
                        result.add(cleanupEntityDirectory(entityDir));
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.error("Failed to cleanup user directory: %s", userDir, e);
        }

        try {
            if (isDirectoryEmpty(userDir)) {
                Files.delete(userDir);
                LOGGER.debugf("Removed empty user directory: %s", userDir);
            }
        } catch (Exception e) {
            LOGGER.debugf("Could not remove user directory: %s", userDir);
        }

        return result;
    }

    private CleanupResult cleanupTempDirectory(Path tempDir) {
        CleanupResult result = new CleanupResult();

        try {
            if (isSpecialTempDirectory(tempDir.getParent())) {
                // Direct cleanup for audio-processing and playlist-processing
                cleanupTempFiles(tempDir, result);
            } else {
                // Check if it's a temp subdirectory or direct temp files
                if (Files.exists(tempDir.resolve("temp"))) {
                    cleanupTempFiles(tempDir.resolve("temp"), result);
                } else {
                    cleanupTempFiles(tempDir, result);
                }
            }

            if (isDirectoryEmpty(tempDir)) {
                Files.delete(tempDir);
                LOGGER.debugf("Removed empty temp directory: %s", tempDir);
            }

        } catch (Exception e) {
            LOGGER.error("Failed to cleanup temp directory: %s", tempDir, e);
        }

        return result;
    }

    private void cleanupTempFiles(Path directory, CleanupResult result) {
        try (Stream<Path> files = Files.list(directory)) {
            Instant cutoffTime = Instant.now().minus(TEMP_FILE_MAX_AGE);

            for (Path file : files.toArray(Path[]::new)) {
                if (Files.isRegularFile(file)) {
                    try {
                        Instant fileTime = Files.getLastModifiedTime(file).toInstant();
                        if (fileTime.isBefore(cutoffTime)) {
                            long fileSize = Files.size(file);
                            Files.delete(file);
                            result.tempFilesDeleted++;
                            result.bytesFreed += fileSize;
                            LOGGER.debugf("Deleted old temp file: %s", file);
                        }
                    } catch (Exception e) {
                        LOGGER.warn("Failed to delete temp file: %s", file, e);
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.error("Failed to cleanup temp files in directory: %s", directory, e);
        }
    }

    private CleanupResult cleanupEntityDirectory(Path entityDir) {
        CleanupResult result = new CleanupResult();

        try (Stream<Path> files = Files.list(entityDir)) {
            Instant cutoffTime = Instant.now().minus(ENTITY_FILE_MAX_AGE);

            for (Path file : files.toArray(Path[]::new)) {
                if (Files.isRegularFile(file)) {
                    try {
                        Instant fileTime = Files.getLastModifiedTime(file).toInstant();
                        if (fileTime.isBefore(cutoffTime)) {
                            long fileSize = Files.size(file);
                            Files.delete(file);
                            result.entityFilesDeleted++;
                            result.bytesFreed += fileSize;
                            LOGGER.debugf("Deleted old entity file: %s", file);
                        }
                    } catch (Exception e) {
                        LOGGER.warn("Failed to delete entity file: %s", file, e);
                    }
                }
            }

            if (isDirectoryEmpty(entityDir)) {
                Files.delete(entityDir);
                LOGGER.debugf("Removed empty entity directory: %s", entityDir);
            }

        } catch (Exception e) {
            LOGGER.error("Failed to cleanup entity directory: %s", entityDir, e);
        }

        return result;
    }

    private boolean isDirectoryEmpty(Path directory) {
        try (Stream<Path> stream = Files.list(directory)) {
            return stream.findFirst().isEmpty();
        } catch (Exception e) {
            return false;
        }
    }

    public Uni<Void> cleanupTempFilesForUser(String username) {
        return Uni.createFrom().item(() -> {
                    Path tempDir = Paths.get(managedDirectories.getFirst(), username, "temp");
                    if (Files.exists(tempDir)) {
                        cleanupTempDirectory(tempDir);
                        LOGGER.infof("Cleaned up temp files for user: %s", username);
                    }
                    return null;
                }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool())
                .replaceWithVoid();
    }

    public Uni<Void> cleanupEntityFiles(String username, String entityId) {
        return Uni.createFrom().item(() -> {
                    Path entityDir = Paths.get(managedDirectories.getFirst(), username, entityId);
                    if (Files.exists(entityDir)) {
                        CleanupResult result = cleanupEntityDirectory(entityDir);
                        LOGGER.infof("Cleaned up entity files for user: %s, entity: %s - %s files, %s bytes",
                                username, entityId, result.entityFilesDeleted, result.bytesFreed);
                    }
                    return null;
                }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool())
                .replaceWithVoid();
    }

    public Uni<Void> cleanupAfterSuccessfulUpload(String username, String entityId, String fileName) {
        return fileSystem.delete(Paths.get(managedDirectories.get(0), username, entityId, fileName).toString())
                .onItem().invoke(() -> LOGGER.debugf("Cleaned up local file after successful upload: %s/%s/%s",
                        username, entityId, fileName))
                .onFailure().invoke(e -> LOGGER.warnf("Failed to cleanup local file: %s/%s/%s",
                        username, entityId, fileName, e))
                .onFailure().recoverWithNull()
                .replaceWithVoid();
    }

    public CleanupStats getStats() {
        return CleanupStats.builder()
                .tempFilesDeleted(tempFilesDeleted.get())
                .entityFilesDeleted(entityFilesDeleted.get())
                .totalBytesFreed(bytesFreed.get())
                .lastCleanupTime(lastCleanupTime)
                .build();
    }

    private static class CleanupResult {
        long tempFilesDeleted = 0;
        long entityFilesDeleted = 0;
        long bytesFreed = 0;

        void add(CleanupResult other) {
            this.tempFilesDeleted += other.tempFilesDeleted;
            this.entityFilesDeleted += other.entityFilesDeleted;
            this.bytesFreed += other.bytesFreed;
        }
    }

    @Getter
    public static class CleanupStats {
        private final long tempFilesDeleted;
        private final long entityFilesDeleted;
        private final long totalBytesFreed;
        private final LocalDateTime lastCleanupTime;

        private CleanupStats(long tempFilesDeleted, long entityFilesDeleted,
                             long totalBytesFreed, LocalDateTime lastCleanupTime) {
            this.tempFilesDeleted = tempFilesDeleted;
            this.entityFilesDeleted = entityFilesDeleted;
            this.totalBytesFreed = totalBytesFreed;
            this.lastCleanupTime = lastCleanupTime;
        }

        public static CleanupStatsBuilder builder() {
            return new CleanupStatsBuilder();
        }

        public static class CleanupStatsBuilder {
            private long tempFilesDeleted;
            private long entityFilesDeleted;
            private long totalBytesFreed;
            private LocalDateTime lastCleanupTime;

            public CleanupStatsBuilder tempFilesDeleted(long tempFilesDeleted) {
                this.tempFilesDeleted = tempFilesDeleted;
                return this;
            }

            public CleanupStatsBuilder entityFilesDeleted(long entityFilesDeleted) {
                this.entityFilesDeleted = entityFilesDeleted;
                return this;
            }

            public CleanupStatsBuilder totalBytesFreed(long totalBytesFreed) {
                this.totalBytesFreed = totalBytesFreed;
                return this;
            }

            public CleanupStatsBuilder lastCleanupTime(LocalDateTime lastCleanupTime) {
                this.lastCleanupTime = lastCleanupTime;
                return this;
            }

            public CleanupStats build() {
                return new CleanupStats(tempFilesDeleted, entityFilesDeleted, totalBytesFreed, lastCleanupTime);
            }
        }
    }
}