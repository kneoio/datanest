package com.semantyca.datanest.service.util;

import com.semantyca.core.model.FileData;
import com.semantyca.core.model.user.IUser;
import com.semantyca.core.util.FileSecurityUtils;
import com.semantyca.datanest.config.DatanestConfig;
import com.semantyca.datanest.service.soundfragment.SoundFragmentService;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;

@ApplicationScoped
public class FileDownloadService {
    private static final Logger LOGGER = Logger.getLogger(FileDownloadService.class);

    private final String uploadDir;
    private final SoundFragmentService soundFragmentService;

    @Inject
    public FileDownloadService(DatanestConfig config, SoundFragmentService soundFragmentService) {
        this.uploadDir = config.getPathUploads() + "/sound-fragments-controller";
        this.soundFragmentService = soundFragmentService;
    }

    public Uni<FileData> getFile(String id, String requestedFileName, IUser user) {
        try {
            if ("temp".equals(id)) {
                return getTempFile(requestedFileName, user);
            }

            return getEntityFile(id, requestedFileName, user);

        } catch (SecurityException | IllegalArgumentException e) {
            return Uni.createFrom().failure(e);
        }
    }

    private Uni<FileData> getTempFile(String requestedFileName, IUser user) {
        String safeFileName;
        try {
            safeFileName = FileSecurityUtils.sanitizeFilename(requestedFileName);
        } catch (SecurityException e) {
            LOGGER.warnf("Unsafe filename in temp file request: %s from user: %s", requestedFileName,
                    user.getUserName());
            return Uni.createFrom().failure(new SecurityException("Invalid filename"));
        }

        Path baseDir = Paths.get(uploadDir, user.getUserName(), "temp");
        Path secureFilePath = FileSecurityUtils.secureResolve(baseDir, safeFileName);

        if (!FileSecurityUtils.isPathWithinBase(baseDir, secureFilePath)) {
            LOGGER.errorf("Security violation: Path traversal attempt by user %s for temp file %s",
                    user.getUserName(), requestedFileName);
            return Uni.createFrom().failure(new SecurityException("Invalid file path"));
        }

        File file = secureFilePath.toFile();
        if (file.exists()) {
            try {
                Path canonicalFile = file.toPath().toRealPath();
                Path canonicalBase = baseDir.toRealPath();
                if (!canonicalFile.startsWith(canonicalBase)) {
                    LOGGER.errorf("Security violation: Temp file outside base directory accessed by user %s",
                            user.getUserName());
                    return Uni.createFrom().failure(new SecurityException("File access denied"));
                }

                byte[] fileBytes = Files.readAllBytes(canonicalFile);
                String mimeType = Files.probeContentType(canonicalFile);
                return Uni.createFrom().item(new FileData(
                        fileBytes,
                        mimeType != null ? mimeType : "application/octet-stream"));
            } catch (IOException e) {
                LOGGER.errorf("Temp file read error for user %s, file: %s", user.getUserName(), safeFileName, e);
                return Uni.createFrom().failure(e);
            }
        } else {
            return Uni.createFrom().failure(new FileNotFoundException("Temp file not found: " + safeFileName));
        }
    }

    private Uni<FileData> getEntityFile(String id, String requestedFileName, IUser user) {
        try {
            UUID.fromString(id);
        } catch (IllegalArgumentException e) {
            LOGGER.warnf("Invalid entity ID in file request: %s from user: %s", id, user.getUserName());
            return Uni.createFrom().failure(new IllegalArgumentException("Invalid entity ID"));
        }

        String safeFileName;
        try {
            safeFileName = FileSecurityUtils.sanitizeFilename(requestedFileName);
        } catch (SecurityException e) {
            LOGGER.warnf("Unsafe filename in file request: %s from user: %s", requestedFileName, user.getUserName());
            return Uni.createFrom().failure(new SecurityException("Invalid filename"));
        }

        Path baseDir = Paths.get(uploadDir, user.getUserName(), id);
        Path secureFilePath = FileSecurityUtils.secureResolve(baseDir, safeFileName);

        if (!FileSecurityUtils.isPathWithinBase(baseDir, secureFilePath)) {
            LOGGER.errorf("Security violation: Path traversal attempt by user %s for file %s",
                    user.getUserName(), requestedFileName);
            return Uni.createFrom().failure(new SecurityException("Invalid file path"));
        }

        File file = secureFilePath.toFile();

        if (file.exists()) {
            try {
                Path canonicalFile = file.toPath().toRealPath();
                Path canonicalBase = baseDir.toRealPath();
                if (!canonicalFile.startsWith(canonicalBase)) {
                    LOGGER.errorf("Security violation: File outside base directory accessed by user %s",
                            user.getUserName());
                    return Uni.createFrom().failure(new SecurityException("File access denied"));
                }

                byte[] fileBytes = Files.readAllBytes(canonicalFile);
                String mimeType = Files.probeContentType(canonicalFile);
                return Uni.createFrom().item(new FileData(
                        fileBytes,
                        mimeType != null ? mimeType : "application/octet-stream"));
            } catch (IOException e) {
                LOGGER.errorf("File read error for user %s, file: %s", user.getUserName(), safeFileName, e);
                return Uni.createFrom().failure(e);
            }
        }

        return soundFragmentService.getFileBySlugName(UUID.fromString(id), safeFileName, user)
                .onItem()
                .transform(fileMetadata -> {
                    // Cloud storage file - use InputStream
                    return new FileData(fileMetadata.getInputStream(), fileMetadata.getMimeType(), 
                                       fileMetadata.getContentLength() != null ? fileMetadata.getContentLength() : 0);
                });
    }
}