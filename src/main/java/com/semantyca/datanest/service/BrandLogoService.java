package com.semantyca.datanest.service;

import com.semantyca.core.model.FileMetadata;
import com.semantyca.core.model.user.IUser;
import com.semantyca.core.repository.IFileStorage;
import com.semantyca.core.service.external.hetzner.HetznerStorageService;
import com.semantyca.core.util.FileSecurityUtils;
import com.semantyca.datanest.config.DatanestConfig;
import com.semantyca.datanest.repository.BrandRepository;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.vertx.ext.web.RoutingContext;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Set;
import java.util.UUID;

@ApplicationScoped
public class BrandLogoService {
    private static final Logger LOGGER = LoggerFactory.getLogger(BrandLogoService.class);
    private static final long MAX_LOGO_SIZE_BYTES = 10 * 1024 * 1024; // 10 MB
    private static final String CONTROLLER_KEY = "brands-controller";

    private static final Set<String> SUPPORTED_IMAGE_EXTENSIONS = Set.of("jpg", "jpeg", "png", "gif", "webp", "svg");
    private static final Set<String> SUPPORTED_IMAGE_MIME_TYPES = Set.of(
            "image/jpeg", "image/png", "image/gif", "image/webp", "image/svg+xml"
    );

    private final String uploadDirectory;
    private final BrandRepository brandRepository;
    private final IFileStorage fileStorage;

    @Inject
    public BrandLogoService(DatanestConfig config, BrandRepository brandRepository, HetznerStorageService fileStorage) {
        this.uploadDirectory = config.getPathUploads();
        this.brandRepository = brandRepository;
        this.fileStorage = fileStorage;
    }

    public Uni<FileMetadata> uploadLogo(RoutingContext rc, UUID brandId, IUser user) {
        return Uni.createFrom().<FileMetadata>emitter(emitter -> {
            try {
                rc.request().setExpectMultipart(true);
                rc.request().uploadHandler(upload -> {
                    try {
                        String originalFileName = upload.filename();
                        String contentType = upload.contentType();

                        if (!isValidImageFile(originalFileName, contentType)) {
                            emitter.fail(new IllegalArgumentException(
                                    "Unsupported file type. Allowed: " + String.join(", ", SUPPORTED_IMAGE_EXTENSIONS)));
                            return;
                        }

                        String safeFileName = FileSecurityUtils.sanitizeFilename(originalFileName);
                        Path entityDir = Files.createDirectories(
                                Paths.get(uploadDirectory, CONTROLLER_KEY, user.getUserName(), brandId.toString()));
                        Path destination = FileSecurityUtils.secureResolve(entityDir, safeFileName);

                        if (!FileSecurityUtils.isPathWithinBase(entityDir, destination)) {
                            emitter.fail(new SecurityException("Invalid file path"));
                            return;
                        }

                        Path tempFile = entityDir.resolve(".tmp_" + safeFileName);
                        upload.streamToFileSystem(tempFile.toString());

                        upload.endHandler(v -> {
                            try {
                                if (!Files.exists(tempFile)) {
                                    emitter.fail(new IOException("Temp file missing after upload"));
                                    return;
                                }
                                if (Files.size(tempFile) > MAX_LOGO_SIZE_BYTES) {
                                    Files.deleteIfExists(tempFile);
                                    emitter.fail(new IllegalArgumentException("Logo file too large. Max size is 10 MB"));
                                    return;
                                }
                                Files.move(tempFile, destination, StandardCopyOption.REPLACE_EXISTING);

                                String slugName = safeFileName;
                                FileMetadata meta = new FileMetadata();
                                meta.setMimeType(contentType != null ? contentType : Files.probeContentType(destination));
                                meta.setFileOriginalName(originalFileName);
                                meta.setSlugName(slugName);

                                brandRepository.findById(brandId, user, false)
                                        .chain(brand -> {
                                            meta.setFileKey("logo/" + brand.getSlugName() + "/" + slugName);
                                            return fileStorage.uploadFile(meta.getFileKey(), destination.toString(), meta.getMimeType());
                                        })
                                        .chain(ignored -> brandRepository.upsertLogoFile(brandId, meta))
                                        .subscribe().with(
                                                ignored -> emitter.complete(meta),
                                                emitter::fail
                                        );
                            } catch (Exception e) {
                                try { Files.deleteIfExists(tempFile); } catch (IOException ignored) {}
                                emitter.fail(e);
                            }
                        });

                        upload.exceptionHandler(err -> {
                            try { Files.deleteIfExists(tempFile); } catch (IOException ignored) {}
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

    public Uni<byte[]> getLogo(UUID brandId, String slugName, IUser user) {
        String safeFileName;
        try {
            safeFileName = FileSecurityUtils.sanitizeFilename(slugName);
        } catch (SecurityException e) {
            return Uni.createFrom().failure(new SecurityException("Invalid filename"));
        }

        Path baseDir = Paths.get(uploadDirectory, CONTROLLER_KEY, user.getUserName(), brandId.toString());
        Path filePath = FileSecurityUtils.secureResolve(baseDir, safeFileName);

        if (!FileSecurityUtils.isPathWithinBase(baseDir, filePath)) {
            return Uni.createFrom().failure(new SecurityException("Invalid file path"));
        }

        if (filePath.toFile().exists()) {
            return Uni.createFrom().item(() -> {
                try {
                    return Files.readAllBytes(filePath.toRealPath());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
        }

        return brandRepository.getLogoFileBySlugName(brandId, safeFileName)
                .onItem().transformToUni(meta -> Uni.createFrom().failure(
                        new java.io.FileNotFoundException("Logo file not found on filesystem: " + slugName)));
    }

    public Uni<FileMetadata> getLogoMetadata(UUID brandId, String slugName) {
        return brandRepository.getLogoFileBySlugName(brandId, slugName);
    }

    private boolean isValidImageFile(String filename, String contentType) {
        if (filename == null || filename.trim().isEmpty()) return false;
        String ext = getExtension(filename.toLowerCase());
        boolean validExt = SUPPORTED_IMAGE_EXTENSIONS.contains(ext);
        boolean validMime = contentType != null && SUPPORTED_IMAGE_MIME_TYPES.stream().anyMatch(contentType::startsWith);
        return validExt || validMime;
    }

    private String getExtension(String filename) {
        int dot = filename.lastIndexOf('.');
        return (dot > 0 && dot < filename.length() - 1) ? filename.substring(dot + 1) : "";
    }
}
