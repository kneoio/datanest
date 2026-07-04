package com.semantyca.datanest.rest;

import com.semantyca.core.controller.AbstractSecuredController;
import com.semantyca.core.dto.rls.RlsActionDTO;
import com.semantyca.core.repository.exception.UserNotFoundException;
import com.semantyca.core.service.UserService;
import com.semantyca.datanest.dto.BulkACLRequestDTO;
import com.semantyca.datanest.service.BulkACLService;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;

@ApplicationScoped
public class BulkACLController extends AbstractSecuredController<Object, BulkACLRequestDTO> {
    private static final Logger LOGGER = LoggerFactory.getLogger(BulkACLController.class);

    private final BulkACLService bulkACLService;

    public BulkACLController() {
        super(null);
        this.bulkACLService = null;
    }

    @Inject
    public BulkACLController(UserService userService, BulkACLService bulkACLService) {
        super(userService);
        this.bulkACLService = bulkACLService;
    }

    public void setupRoutes(Router router) {
        String path = "/datanest/bulk-acl";
        router.route(HttpMethod.POST, path + "/start").handler(BodyHandler.create()).handler(this::startJob);
        router.route(HttpMethod.GET, path + "/stream/:jobId").handler(this::streamProgress);
    }

    private void startJob(RoutingContext rc) {
        try {
            String jobId = rc.request().getParam("jobId");
            if (jobId == null || jobId.isBlank()) {
                rc.fail(400, new IllegalArgumentException("jobId query parameter is required"));
                return;
            }

            JsonObject body = rc.body().asJsonObject();
            if (body == null) {
                rc.fail(400, new IllegalArgumentException("Request body is required"));
                return;
            }

            JsonArray docIdsArray = body.getJsonArray("documentIds");
            JsonArray actionsArray = body.getJsonArray("actions");

            if (docIdsArray == null || docIdsArray.isEmpty()) {
                rc.fail(400, new IllegalArgumentException("documentIds is required and must not be empty"));
                return;
            }

            if (actionsArray == null || actionsArray.isEmpty()) {
                rc.fail(400, new IllegalArgumentException("actions is required and must not be empty"));
                return;
            }

            List<UUID> documentIds = new ArrayList<>();
            for (int i = 0; i < docIdsArray.size(); i++) {
                try {
                    documentIds.add(UUID.fromString(docIdsArray.getString(i)));
                } catch (IllegalArgumentException e) {
                    rc.fail(400, new IllegalArgumentException("Invalid UUID in documentIds at index " + i));
                    return;
                }
            }

            List<RlsActionDTO> actions = new ArrayList<>();
            for (int i = 0; i < actionsArray.size(); i++) {
                actions.add(actionsArray.getJsonObject(i).mapTo(RlsActionDTO.class));
            }

            String resourceType = body.getString("resourceType");
            if (resourceType == null || resourceType.isBlank()) {
                rc.fail(400, new IllegalArgumentException("resourceType is required"));
                return;
            }

            BulkACLRequestDTO request = new BulkACLRequestDTO();
            request.setDocumentIds(documentIds);
            request.setActions(actions);
            request.setResourceType(resourceType);

            getContextUser(rc, false, true)
                    .subscribe().with(
                            user -> {
                                assert bulkACLService != null;
                                bulkACLService.startJob(jobId, request, user);
                                rc.response()
                                        .setStatusCode(202)
                                        .putHeader("Content-Type", "application/json")
                                        .end(new JsonObject().put("jobId", jobId).encode());
                            },
                            t -> rc.fail(500, t)
                    );

        } catch (Exception e) {
            LOGGER.error("Error starting bulk ACL job", e);
            rc.fail(400, new IllegalArgumentException("Invalid request: " + e.getMessage()));
        }
    }

    private void streamProgress(RoutingContext rc) {
        String jobId = rc.pathParam("jobId");
        if (jobId == null || jobId.isBlank()) {
            rc.fail(400, new IllegalArgumentException("jobId is required"));
            return;
        }

        var resp = rc.response();
        resp.setChunked(true);
        resp.putHeader("Content-Type", "text/event-stream");
        resp.putHeader("Cache-Control", "no-cache");
        resp.putHeader("Connection", "keep-alive");

        Consumer<BulkACLService.SseEvent> consumer = ev -> {
            try {
                String frame = "event: " + ev.type() + '\n' +
                        "data: " + (ev.data() != null ? ev.data().encode() : "{}") + '\n' + '\n';
                resp.write(frame);
                if ("done".equals(ev.type()) || "error".equals(ev.type())) {
                    resp.end();
                }
            } catch (Exception ignored) { }
        };

        assert bulkACLService != null;
        bulkACLService.subscribe(jobId, consumer);

        resp.exceptionHandler(t -> bulkACLService.unsubscribe(jobId, consumer));
        resp.endHandler(v -> bulkACLService.unsubscribe(jobId, consumer));
    }

    @Override
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
