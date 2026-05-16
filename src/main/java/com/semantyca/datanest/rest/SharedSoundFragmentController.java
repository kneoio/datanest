package com.semantyca.datanest.rest;

import com.semantyca.core.controller.AbstractSecuredController;
import com.semantyca.core.dto.cnst.PayloadType;
import com.semantyca.core.dto.view.View;
import com.semantyca.core.dto.view.ViewPage;
import com.semantyca.core.repository.exception.UserNotFoundException;
import com.semantyca.core.service.UserService;
import com.semantyca.core.util.RuntimeUtil;
import com.semantyca.datanest.dto.sharing.ShareDTO;
import com.semantyca.datanest.dto.SharePatchDTO;
import com.semantyca.datanest.dto.sharing.SharingPreviewDTO;
import com.semantyca.datanest.model.soundfragment.SharedSoundFragment;
import com.semantyca.datanest.service.soundfragment.SharedSoundFragmentService;
import io.smallrye.mutiny.Uni;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.validation.Validator;

import java.util.UUID;

@ApplicationScoped
public class SharedSoundFragmentController extends AbstractSecuredController<SharedSoundFragment, ShareDTO> {

    private SharedSoundFragmentService sharedSoundFragmentService;
    private Validator validator;

    public SharedSoundFragmentController() {
        super(null);
    }

    @Inject
    public SharedSoundFragmentController(UserService userService,
                                         SharedSoundFragmentService sharedSoundFragmentService,
                                         Validator validator) {
        super(userService);
        this.sharedSoundFragmentService = sharedSoundFragmentService;
        this.validator = validator;
    }

    public void setupRoutes(Router router) {
        String path = "/datanest/shared-sound-fragments";
        BodyHandler jsonBodyHandler = BodyHandler.create().setHandleFileUploads(false);
        // sharer adds or removes target brands for a shared fragment
        router.route(HttpMethod.PATCH,  path + "/shared/:fragmentId").handler(jsonBodyHandler).handler(this::patchToShare);
        // receiver: list all shares sent to the current user
        router.route(HttpMethod.GET,    path + "/received").handler(this::getReceived);
        // receiver: get a single received share by id
        router.route(HttpMethod.GET,    path + "/received/:id").handler(this::getReceivedDoc);
        // receiver: reject a received share — removes RLS access, marks status 501, main record stays
        router.route(HttpMethod.DELETE, path + "/received/:id").handler(this::rejectShareByReceiver);

        //--- only for 42next ---
        router.route(HttpMethod.GET,    path + "/received/:id/access").handler(this::getDocumentAccess);
        // sender/admin: archive a share — sets archived=1, excluded from sharedWith going forward
        router.route(HttpMethod.DELETE, path + "/shared/:id").handler(this::delete);
    }

    private void getReceived(RoutingContext rc) {
        int page = Integer.parseInt(rc.request().getParam("page", "1"));
        int size = Integer.parseInt(rc.request().getParam("size", "10"));

        getContextUser(rc, false, true)
                .chain(user -> Uni.combine().all().unis(
                        sharedSoundFragmentService.getSharingPreviewCount(user),
                        sharedSoundFragmentService.getSharingPreviewList(size, (page - 1) * size, user)
                ).asTuple().map(tuple -> {
                    ViewPage viewPage = new ViewPage();
                    View<SharingPreviewDTO> dtoEntries = new View<>(tuple.getItem2(),
                            tuple.getItem1(), page,
                            RuntimeUtil.countMaxPage(tuple.getItem1(), size),
                            size);
                    viewPage.addPayload(PayloadType.VIEW_DATA, dtoEntries);
                    return viewPage;
                }))
                .subscribe().with(
                        viewPage -> rc.response()
                                .setStatusCode(200)
                                .putHeader("Content-Type", "application/json")
                                .end(io.vertx.core.json.Json.encode(viewPage)),
                        t -> handleFailure(rc, t)
                );
    }

    private void getReceivedDoc(RoutingContext rc) {
        UUID id = UUID.fromString(rc.pathParam("id"));
        getContextUser(rc, false, true)
                .chain(user -> sharedSoundFragmentService.getById(id, user))
                .subscribe().with(
                        dto -> rc.response()
                                .setStatusCode(200)
                                .putHeader("Content-Type", "application/json")
                                .end(JsonObject.mapFrom(dto).encode()),
                        t -> handleFailure(rc, t)
                );
    }

    private void rejectShareByReceiver(RoutingContext rc) {
        UUID shareId = UUID.fromString(rc.pathParam("id"));
        getContextUser(rc, false, true)
                .chain(user -> sharedSoundFragmentService.rejectShareByReceiver(shareId, user))
                .subscribe().with(
                        count -> rc.response().setStatusCode(204).end(),
                        t -> handleFailure(rc, t)
                );
    }

    private void patchToShare(RoutingContext rc) {
        try {
            if (!validateJsonBody(rc)) return;

            UUID fragmentId = UUID.fromString(rc.pathParam("fragmentId"));
            SharePatchDTO patch = rc.body().asJsonObject().mapTo(SharePatchDTO.class);
            if (!validateDTO(rc, patch, validator)) return;

            getContextUser(rc, false, true)
                    .chain(user -> sharedSoundFragmentService.patchShares(fragmentId, patch, user)
                            .chain(() -> sharedSoundFragmentService.listSharedSoundFragmentDTO(fragmentId)))
                    .subscribe().with(
                            shares -> rc.response()
                                    .setStatusCode(200)
                                    .putHeader("Content-Type", "application/json")
                                    .end(io.vertx.core.json.Json.encode(shares)),
                            t -> handleFailure(rc, t)
                    );
        } catch (Exception e) {
            if (e instanceof IllegalArgumentException) {
                rc.fail(400, e);
            } else {
                rc.fail(400, new IllegalArgumentException("Invalid JSON payload"));
            }
        }
    }

    private void delete(RoutingContext rc) {
        UUID shareId = UUID.fromString(rc.pathParam("id"));
        getContextUser(rc, false, true)
                .chain(user -> sharedSoundFragmentService.delete(shareId, user))
                .subscribe().with(
                        count -> rc.response().setStatusCode(204).end(),
                        t -> handleFailure(rc, t)
                );
    }

    private void getDocumentAccess(RoutingContext rc) {
        String id = rc.pathParam("id");

        try {
            UUID documentId = UUID.fromString(id);

            getContextUser(rc, false, true)
                    .chain(user -> sharedSoundFragmentService.getDocumentAccess(documentId, user))
                    .subscribe().with(
                            accessList -> {
                                JsonObject response = new JsonObject();
                                response.put("documentId", id);
                                response.put("accessList", accessList);
                                rc.response()
                                        .setStatusCode(200)
                                        .putHeader("Content-Type", "application/json")
                                        .end(response.encode());
                            },
                            t -> handleFailure(rc, t)
                    );
        } catch (IllegalArgumentException e) {
            rc.fail(400, new IllegalArgumentException("Invalid document ID format"));
        }
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
