package com.semantyca.datanest.rest;

import com.semantyca.core.controller.AbstractSecuredController;
import com.semantyca.core.repository.exception.UserNotFoundException;
import com.semantyca.core.service.UserService;
import com.semantyca.datanest.dto.SoundFragmentDTO;
import com.semantyca.datanest.dto.UploadFileDTO;
import com.semantyca.datanest.service.util.FileUploadService;
import com.semantyca.mixpla.model.soundfragment.SoundFragment;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@ApplicationScoped
public class SoundFragmentBulkUploadController extends AbstractSecuredController<SoundFragment, SoundFragmentDTO> {
    private static final Logger LOGGER = LoggerFactory.getLogger(SoundFragmentBulkUploadController.class);

    private static final long BULK_SSE_POLL_MS = 500L;
    private static final long BULK_SSE_KEEPALIVE_MS = 30_000L;

    private FileUploadService fileUploadService;
    private Vertx vertx;

    public SoundFragmentBulkUploadController() {
        super(null);
    }

    @Inject
    public SoundFragmentBulkUploadController(UserService userService,
                                             FileUploadService fileUploadService,
                                             Vertx vertx) {
        super(userService);
        this.fileUploadService = fileUploadService;
        this.vertx = vertx;
    }

    public void setupRoutes(Router router) {
        String path = "/datanest/soundfragments-bulk";
        router.route(HttpMethod.GET, path + "/status/:batchId/stream").handler(this::streamProgress);
        router.route(HttpMethod.POST, path + "/files").handler(this::uploadFile);
        router.route(HttpMethod.POST, path + "/chunk").handler(this::uploadChunk);
    }

    private void uploadFile(RoutingContext rc) {
        String batchId = rc.request().getParam("batchId");
        String brandSlug = rc.request().getParam("brandSlug");
        String fileId = rc.request().getParam("fileId");

        if (batchId == null || batchId.trim().isEmpty()) {
            rc.fail(400, new IllegalArgumentException("batchId parameter is required"));
            return;
        }

        if (fileId == null || fileId.trim().isEmpty()) {
            rc.fail(400, new IllegalArgumentException("fileId parameter is required"));
            return;
        }

        getContextUser(rc, false, true)
                .chain(user -> fileUploadService.processDirectBulkStreamAsync(rc, batchId, fileId, brandSlug, "sound-fragments-controller", user))
                .subscribe().with(
                        dto -> {
                            LOGGER.info("Bulk upload done: {}", batchId);
                            rc.response()
                                    .setStatusCode(200)
                                    .putHeader("Content-Type", "application/json")
                                    .end(io.vertx.core.json.Json.encode(dto));
                        },
                        err -> {
                            LOGGER.error("Bulk upload failed: batchId={}, fileId={}, error={}", batchId, fileId, err.getMessage(), err);
                            if (err instanceof IllegalArgumentException e) {
                                int status;
                                if (e.getMessage() != null && e.getMessage().contains("Unsupported")) {
                                    status = 415;
                                } else {
                                    status = 400;
                                }
                                rc.fail(status, e);
                            } else {
                                rc.fail(500, new RuntimeException("Upload failed"));
                            }
                        }
                );
    }


    private void uploadChunk(RoutingContext rc) {
        String batchId = rc.request().getParam("batchId");
        String fileId = rc.request().getParam("fileId");
        String fileName = rc.request().getParam("fileName");
        String brandSlug = rc.request().getParam("brandSlug");
        String chunkIndexStr = rc.request().getParam("chunkIndex");
        String totalChunksStr = rc.request().getParam("totalChunks");

        if (batchId == null || batchId.isBlank()) { rc.fail(400, new IllegalArgumentException("batchId required")); return; }
        if (fileId == null || fileId.isBlank())   { rc.fail(400, new IllegalArgumentException("fileId required")); return; }
        if (fileName == null || fileName.isBlank()) { rc.fail(400, new IllegalArgumentException("fileName required")); return; }
        if (chunkIndexStr == null || totalChunksStr == null) { rc.fail(400, new IllegalArgumentException("chunkIndex and totalChunks required")); return; }

        int chunkIndex, totalChunks;
        try {
            chunkIndex = Integer.parseInt(chunkIndexStr);
            totalChunks = Integer.parseInt(totalChunksStr);
        } catch (NumberFormatException e) {
            rc.fail(400, new IllegalArgumentException("chunkIndex and totalChunks must be integers"));
            return;
        }
        if (chunkIndex < 0 || totalChunks < 1 || chunkIndex >= totalChunks) {
            rc.fail(400, new IllegalArgumentException("Invalid chunk parameters"));
            return;
        }

        final int ci = chunkIndex, tc = totalChunks;
        getContextUser(rc, false, true)
                .chain(user -> fileUploadService.processChunkUpload(rc, batchId, fileId, ci, tc, fileName, brandSlug, "sound-fragments-controller", user))
                .subscribe().with(
                        dto -> rc.response()
                                .setStatusCode(200)
                                .putHeader("Content-Type", "application/json")
                                .end(io.vertx.core.json.Json.encode(dto)),
                        err -> {
                            LOGGER.error("Chunk upload failed: batchId={}, fileId={}, chunk={}/{}, error={}",
                                    batchId, fileId, chunkIndex, totalChunks, err.getMessage(), err);
                            if (err instanceof IllegalArgumentException e) {
                                rc.fail(400, e);
                            } else {
                                rc.fail(500, new RuntimeException("Chunk upload failed"));
                            }
                        }
                );
    }

    private void streamProgress(RoutingContext rc) {
        String batchId = rc.pathParam("batchId");

        rc.response()
                .putHeader("Content-Type", "text/event-stream")
                .putHeader("Cache-Control", "no-cache")
                .setChunked(true);

        ConcurrentHashMap<String, UploadFileDTO> snapshot = fileUploadService.getBulkUploadProgress(batchId);
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
            ConcurrentHashMap<String, UploadFileDTO> files = fileUploadService.getBulkUploadProgress(batchId);
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

    protected void handleFailure(RoutingContext rc, Throwable throwable) {
        if (throwable instanceof IllegalStateException
                || throwable instanceof IllegalArgumentException
                || throwable instanceof UserNotFoundException) {
            rc.fail(401, throwable);
        } else {
            rc.fail(throwable);
        }
    }
}
