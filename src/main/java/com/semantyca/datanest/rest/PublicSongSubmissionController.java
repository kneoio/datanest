package com.semantyca.datanest.rest;

import com.semantyca.core.model.user.SuperUser;
import com.semantyca.datanest.dto.GenreFlatDTO;
import com.semantyca.datanest.dto.PublicSubmissionMetaDTO;
import com.semantyca.datanest.dto.StationFlatDTO;
import com.semantyca.datanest.service.BrandService;
import com.semantyca.datanest.service.OtpService;
import com.semantyca.datanest.service.RefService;
import com.semantyca.datanest.service.util.FileUploadService;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

@ApplicationScoped
public class PublicSongSubmissionController {
    private static final Logger LOGGER = Logger.getLogger(PublicSongSubmissionController.class);
    private static final String CONTROLLER_KEY = "public-submissions";

    private final OtpService otpService;
    private final FileUploadService fileUploadService;
    private final BrandService brandService;
    private final RefService refService;

    @Inject
    public PublicSongSubmissionController(OtpService otpService, FileUploadService fileUploadService, BrandService brandService, RefService refService) {
        this.otpService = otpService;
        this.fileUploadService = fileUploadService;
        this.brandService = brandService;
        this.refService = refService;
    }

    public void setupRoutes(Router router) {
        String base = "/datanest/public";
        router.get(base + "/stations").handler(this::getStations);
        router.post(base + "/songs/request-code").handler(BodyHandler.create()).handler(this::requestCode);
        router.post(base + "/songs/upload").handler(this::upload);
        router.post(base + "/songs/chunk").handler(this::uploadChunk);
    }

    private void getStations(RoutingContext rc) {
        int page = Integer.parseInt(rc.request().getParam("page", "1"));
        int size = Integer.parseInt(rc.request().getParam("size", "10"));
        SuperUser superUser = SuperUser.build();

        Uni.combine().all().unis(
                        brandService.getAllOpenForSubmissionDTO(size, (page - 1) * size, superUser),
                        refService.getAllGenresFlat(1000, 0)
                )
                .asTuple()
                .map(tuple -> {
                    Map<UUID, GenreFlatDTO> genreMap = tuple.getItem2().stream()
                            .collect(Collectors.toMap(GenreFlatDTO::getId, Function.identity()));
                    return tuple.getItem1().stream()
                            .map(b -> StationFlatDTO.from(b, genreMap))
                            .toList();
                })
                .subscribe().with(
                        list -> rc.response()
                                .setStatusCode(200)
                                .putHeader("Content-Type", "application/json")
                                .end(io.vertx.core.json.Json.encode(list)),
                        err -> fail(rc, 500, "Failed to load stations")
                );
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
                            LOGGER.errorf("Failed to send OTP to %s: %s", email, err.getMessage());
                            fail(rc, 500, "Failed to send email");
                        }
                );
    }

    private void upload(RoutingContext rc) {
        String email = rc.request().getParam("email");
        String code  = rc.request().getParam("code");
        String stationSlug = rc.request().getParam("stationSlug");
        String artistName = rc.request().getParam("artistName");
        String description = rc.request().getParam("description");

        if (email == null || code == null) {
            fail(rc, 400, "email and code are required");
            return;
        }
        if (stationSlug == null || stationSlug.isBlank()) {
            fail(rc, 400, "stationSlug is required");
            return;
        }
        if (otpService.isVerifyFail(email.trim(), code.trim())) {
            fail(rc, 401, "Invalid or expired confirmation code");
            return;
        }

        String batchId = UUID.randomUUID().toString();
        String fileId  = UUID.randomUUID().toString();
        PublicSubmissionMetaDTO meta = new PublicSubmissionMetaDTO(email.trim(), artistName, description);

        fileUploadService.processDirectBulkStreamAsync(rc, batchId, fileId, stationSlug, CONTROLLER_KEY, SuperUser.build(), meta)
                .subscribe().with(
                        dto -> rc.response().setStatusCode(200)
                                .putHeader("Content-Type", "application/json")
                                .end(new JsonObject()
                                        .put("message", "Song uploaded successfully. Thank you!")
                                        .put("id", dto.getId())
                                        .put("status", dto.getStatus())
                                        .encode()),
                        err -> {
                            LOGGER.errorf("Public upload failed: %s", err.getMessage());
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
        String stationSlug    = rc.request().getParam("stationSlug");
        String artistName     = rc.request().getParam("artistName");
        String description    = rc.request().getParam("description");

        if (email == null || code == null)           { fail(rc, 400, "email and code are required"); return; }
        if (batchId == null || batchId.isBlank())    { fail(rc, 400, "batchId required"); return; }
        if (fileId == null || fileId.isBlank())      { fail(rc, 400, "fileId required"); return; }
        if (fileName == null || fileName.isBlank())  { fail(rc, 400, "fileName required"); return; }
        if (chunkIndexStr == null || totalChunksStr == null) { fail(rc, 400, "chunkIndex and totalChunks required"); return; }
        if (stationSlug == null || stationSlug.isBlank())    { fail(rc, 400, "stationSlug is required"); return; }

        if (otpService.isVerifyFail(email.trim(), code.trim())) {
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
        PublicSubmissionMetaDTO meta = new PublicSubmissionMetaDTO(email.trim(), artistName, description);
        fileUploadService.processChunkUpload(rc, batchId, fileId, ci, tc, fileName, null, stationSlug, CONTROLLER_KEY, SuperUser.build(), meta)
                .subscribe().with(
                        dto -> rc.response().setStatusCode(200)
                                .putHeader("Content-Type", "application/json")
                                .end(io.vertx.core.json.Json.encode(dto)),
                        err -> {
                            LOGGER.errorf("Public chunk upload failed: %s", err.getMessage());
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
