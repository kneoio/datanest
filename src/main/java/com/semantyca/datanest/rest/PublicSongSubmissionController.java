package com.semantyca.datanest.rest;

import com.semantyca.core.model.user.SuperUser;
import com.semantyca.datanest.service.OtpService;
import com.semantyca.datanest.service.util.FileUploadService;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

@ApplicationScoped
public class PublicSongSubmissionController {
    private static final Logger LOGGER = LoggerFactory.getLogger(PublicSongSubmissionController.class);
    private static final String CONTROLLER_KEY = "public-submissions";

    private final OtpService otpService;
    private final FileUploadService fileUploadService;

    @Inject
    public PublicSongSubmissionController(OtpService otpService, FileUploadService fileUploadService) {
        this.otpService = otpService;
        this.fileUploadService = fileUploadService;
    }

    public void setupRoutes(Router router) {
        String base = "/datanest/public/songs";
        router.post(base + "/request-code").handler(BodyHandler.create()).handler(this::requestCode);
        router.post(base + "/upload").handler(this::upload);
        router.post(base + "/chunk").handler(this::uploadChunk);
    }

    private void requestCode(RoutingContext rc) {
        JsonObject body;
        try {
            body = rc.body().asJsonObject();
        } catch (Exception e) {
            fail(rc, 400, "Invalid JSON");
            return;
        }
        String email = body == null ? null : body.getString("email");
        if (email == null || !email.contains("@")) {
            fail(rc, 400, "Valid email is required");
            return;
        }
        otpService.sendOtp(email.trim())
                .subscribe().with(
                        v -> rc.response().setStatusCode(200)
                                .putHeader("Content-Type", "application/json")
                                .end(new JsonObject().put("message", "Confirmation code sent to " + email).encode()),
                        err -> {
                            LOGGER.error("Failed to send OTP to {}: {}", email, err.getMessage());
                            fail(rc, 500, "Failed to send email");
                        }
                );
    }

    private void upload(RoutingContext rc) {
        String email = rc.request().getParam("email");
        String code  = rc.request().getParam("code");

        if (email == null || code == null) {
            fail(rc, 400, "email and code are required");
            return;
        }
        if (!otpService.verify(email.trim(), code.trim())) {
            fail(rc, 401, "Invalid or expired confirmation code");
            return;
        }

        String batchId = UUID.randomUUID().toString();
        String fileId  = UUID.randomUUID().toString();

        fileUploadService.processDirectBulkStreamAsync(rc, batchId, fileId, null, CONTROLLER_KEY, SuperUser.build())
                .subscribe().with(
                        dto -> rc.response().setStatusCode(200)
                                .putHeader("Content-Type", "application/json")
                                .end(new JsonObject()
                                        .put("message", "Song uploaded successfully. Thank you!")
                                        .put("id", dto.getId())
                                        .put("status", dto.getStatus())
                                        .encode()),
                        err -> {
                            LOGGER.error("Public upload failed: {}", err.getMessage());
                            int status = err instanceof IllegalArgumentException ? 400 : 500;
                            fail(rc, status, err.getMessage());
                        }
                );
    }

    private void uploadChunk(RoutingContext rc) {
        String email          = rc.request().getParam("email");
        String code           = rc.request().getParam("code");
        String batchId        = rc.request().getParam("batchId");
        String fileId         = rc.request().getParam("fileId");
        String fileName       = rc.request().getParam("fileName");
        String chunkIndexStr  = rc.request().getParam("chunkIndex");
        String totalChunksStr = rc.request().getParam("totalChunks");

        if (email == null || code == null)           { fail(rc, 400, "email and code are required"); return; }
        if (batchId == null || batchId.isBlank())    { fail(rc, 400, "batchId required"); return; }
        if (fileId == null || fileId.isBlank())      { fail(rc, 400, "fileId required"); return; }
        if (fileName == null || fileName.isBlank())  { fail(rc, 400, "fileName required"); return; }
        if (chunkIndexStr == null || totalChunksStr == null) { fail(rc, 400, "chunkIndex and totalChunks required"); return; }

        if (!otpService.verify(email.trim(), code.trim())) {
            fail(rc, 401, "Invalid or expired confirmation code");
            return;
        }

        int chunkIndex, totalChunks;
        try {
            chunkIndex  = Integer.parseInt(chunkIndexStr);
            totalChunks = Integer.parseInt(totalChunksStr);
        } catch (NumberFormatException e) {
            fail(rc, 400, "chunkIndex and totalChunks must be integers");
            return;
        }

        final int ci = chunkIndex, tc = totalChunks;
        fileUploadService.processChunkUpload(rc, batchId, fileId, ci, tc, fileName, null, null, CONTROLLER_KEY, SuperUser.build())
                .subscribe().with(
                        dto -> rc.response().setStatusCode(200)
                                .putHeader("Content-Type", "application/json")
                                .end(io.vertx.core.json.Json.encode(dto)),
                        err -> {
                            LOGGER.error("Public chunk upload failed: {}", err.getMessage());
                            int status = err instanceof IllegalArgumentException ? 400 : 500;
                            fail(rc, status, err.getMessage());
                        }
                );
    }

    private void fail(RoutingContext rc, int status, String message) {
        rc.response().setStatusCode(status)
                .putHeader("Content-Type", "application/json")
                .end(new JsonObject().put("error", message).encode());
    }
}
