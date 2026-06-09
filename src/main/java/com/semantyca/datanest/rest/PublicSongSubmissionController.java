package com.semantyca.datanest.rest;

import com.semantyca.datanest.service.OtpService;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.FileUpload;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Set;
import java.util.UUID;

@ApplicationScoped
public class PublicSongSubmissionController {
    private static final Logger LOGGER = LoggerFactory.getLogger(PublicSongSubmissionController.class);
    private static final Set<String> ALLOWED_EXTENSIONS = Set.of(".mp3", ".wav", ".flac", ".ogg", ".opus", ".aac", ".m4a");
    private static final long MAX_BYTES = 200L * 1024 * 1024;

    private final OtpService otpService;
    private final String uploadDirectory;

    @Inject
    public PublicSongSubmissionController(OtpService otpService,
                                          @ConfigProperty(name = "controller.upload.files.path",
                                                  defaultValue = "${java.io.tmpdir}/uploads") String uploadDirectory) {
        this.otpService = otpService;
        this.uploadDirectory = uploadDirectory;
    }

    public void setupRoutes(Router router) {
        String base = "/datanest/public/songs";
        router.route(base + "*").handler(
                BodyHandler.create()
                        .setUploadsDirectory(System.getProperty("java.io.tmpdir") + "/uploads")
                        .setBodyLimit(MAX_BYTES)
        );
        router.post(base + "/request-code").handler(this::requestCode);
        router.post(base + "/upload").handler(this::upload);
    }

    private void requestCode(RoutingContext rc) {
        JsonObject body;
        try {
            body = rc.body().asJsonObject();
        } catch (Exception e) {
            rc.response().setStatusCode(400)
                    .putHeader("Content-Type", "application/json")
                    .end(new JsonObject().put("error", "Invalid JSON").encode());
            return;
        }

        String email = body == null ? null : body.getString("email");
        if (email == null || !email.contains("@")) {
            rc.response().setStatusCode(400)
                    .putHeader("Content-Type", "application/json")
                    .end(new JsonObject().put("error", "Valid email is required").encode());
            return;
        }

        otpService.sendOtp(email.trim())
                .subscribe().with(
                        v -> rc.response()
                                .setStatusCode(200)
                                .putHeader("Content-Type", "application/json")
                                .end(new JsonObject().put("message", "Confirmation code sent to " + email).encode()),
                        err -> {
                            LOGGER.error("Failed to send OTP to {}: {}", email, err.getMessage());
                            rc.response().setStatusCode(500)
                                    .putHeader("Content-Type", "application/json")
                                    .end(new JsonObject().put("error", "Failed to send email").encode());
                        }
                );
    }

    private void upload(RoutingContext rc) {
        String email = rc.request().getFormAttribute("email");
        String code = rc.request().getFormAttribute("code");
        String title = rc.request().getFormAttribute("title");
        String artist = rc.request().getFormAttribute("artist");

        if (email == null || code == null) {
            rc.response().setStatusCode(400)
                    .putHeader("Content-Type", "application/json")
                    .end(new JsonObject().put("error", "email and code are required").encode());
            return;
        }

        if (!otpService.verify(email.trim(), code.trim())) {
            rc.response().setStatusCode(401)
                    .putHeader("Content-Type", "application/json")
                    .end(new JsonObject().put("error", "Invalid or expired confirmation code").encode());
            return;
        }

        List<FileUpload> uploads = rc.fileUploads();
        if (uploads.isEmpty()) {
            rc.response().setStatusCode(400)
                    .putHeader("Content-Type", "application/json")
                    .end(new JsonObject().put("error", "No audio file uploaded").encode());
            return;
        }

        FileUpload file = uploads.get(0);
        String originalName = file.fileName();
        String ext = originalName.contains(".")
                ? originalName.substring(originalName.lastIndexOf('.')).toLowerCase()
                : "";

        if (!ALLOWED_EXTENSIONS.contains(ext)) {
            rc.response().setStatusCode(415)
                    .putHeader("Content-Type", "application/json")
                    .end(new JsonObject().put("error", "Unsupported file type. Allowed: " + ALLOWED_EXTENSIONS).encode());
            return;
        }

        Uni.createFrom().item(() -> {
            try {
                String safeId = UUID.randomUUID().toString();
                Path dest = Paths.get(uploadDirectory, "submissions", safeId + ext);
                Files.createDirectories(dest.getParent());
                Files.move(Paths.get(file.uploadedFileName()), dest, StandardCopyOption.REPLACE_EXISTING);
                LOGGER.info("Song submitted by {} — file saved as {}", email, dest.getFileName());
                return safeId;
            } catch (Exception e) {
                throw new RuntimeException("Failed to save file", e);
            }
        }).subscribe().with(
                id -> rc.response()
                        .setStatusCode(200)
                        .putHeader("Content-Type", "application/json")
                        .end(new JsonObject()
                                .put("message", "Song uploaded successfully. Thank you!")
                                .put("id", id)
                                .encode()),
                err -> {
                    LOGGER.error("Upload save failed: {}", err.getMessage());
                    rc.response().setStatusCode(500)
                            .putHeader("Content-Type", "application/json")
                            .end(new JsonObject().put("error", "Failed to save uploaded file").encode());
                }
        );
    }
}
