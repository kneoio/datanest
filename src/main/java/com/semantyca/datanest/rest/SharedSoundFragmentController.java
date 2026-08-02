package com.semantyca.datanest.rest;

import com.semantyca.core.controller.AbstractSecuredController;
import com.semantyca.core.dto.cnst.PayloadType;
import com.semantyca.core.dto.view.View;
import com.semantyca.core.dto.view.ViewPage;
import com.semantyca.core.repository.exception.UserNotFoundException;
import com.semantyca.core.service.UserService;
import com.semantyca.core.util.RuntimeUtil;
import com.semantyca.datanest.dto.sharing.ShareDTO;
import com.semantyca.datanest.service.soundfragment.SharedSoundFragmentService;
import com.semantyca.mixpla.model.soundfragment.SharedSoundFragment;
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
        // receiver: get a single received share by id
        router.route(HttpMethod.GET,    path + "/received/:id").handler(this::getReceivedDoc);
        // receiver: accept a received share — sets status 500 (OPEN) and adds song to brand playlist
        router.route(HttpMethod.PATCH,  path + "/received/:id/accept").handler(this::acceptShareByReceiver);
        // receiver: reject a received share — marks status 501, keeps RLS/visibility so it stays
        // in the inbox as a rejected item until the receiver removes it via DELETE below
        router.route(HttpMethod.PATCH,  path + "/received/:id/reject").handler(this::rejectShareByReceiver);
        // receiver: delete an already-rejected share — sets archived=1, only works once status is REJECTED
        router.route(HttpMethod.DELETE, path + "/received/:id").handler(this::archiveRejectedByReceiver);

        //--- only for 42next ---
        router.route(HttpMethod.GET,    path + "/received/:id/access").handler(this::getDocumentAccess);
        // sender/admin: archive a share — sets archived=1, excluded from sharedWith going forward
        router.route(HttpMethod.DELETE, path + "/shared/:id").handler(this::delete);
        // 42next admin: create or update a single share directly (id omitted/"new" -> create)
        router.route(HttpMethod.POST, path + "/shared/:id?").handler(jsonBodyHandler).handler(this::upsertShare);
        // 42next admin: browse all shares (not scoped to the current user's inbox)
        router.route(HttpMethod.GET, path + "/shared").handler(this::getAllShares);
        // 42next admin: fetch a single share for the edit form
        router.route(HttpMethod.GET, path + "/shared/:id").handler(this::getShareById);
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

    private void acceptShareByReceiver(RoutingContext rc) {
        UUID shareId = UUID.fromString(rc.pathParam("id"));
        getContextUser(rc, false, true)
                .chain(user -> sharedSoundFragmentService.acceptShareByReceiver(shareId, user))
                .subscribe().with(
                        count -> rc.response().setStatusCode(204).end(),
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

    private void archiveRejectedByReceiver(RoutingContext rc) {
        UUID shareId = UUID.fromString(rc.pathParam("id"));
        getContextUser(rc, false, true)
                .chain(user -> sharedSoundFragmentService.archiveRejectedShareByReceiver(shareId, user))
                .subscribe().with(
                        count -> rc.response().setStatusCode(count > 0 ? 204 : 404).end(),
                        t -> handleFailure(rc, t)
                );
    }

    private void upsertShare(RoutingContext rc) {
        try {
            if (!validateJsonBody(rc)) return;

            com.semantyca.datanest.dto.sharing.ShareAdminDTO dto =
                    rc.body().asJsonObject().mapTo(com.semantyca.datanest.dto.sharing.ShareAdminDTO.class);
            if (!validateDTO(rc, dto, validator)) return;
            String id = rc.pathParam("id");

            getContextUser(rc, false, true)
                    .chain(user -> sharedSoundFragmentService.upsert(id, dto))
                    .subscribe().with(
                            doc -> sendUpsertResponse(rc, doc, id),
                            throwable -> handleUpsertFailure(rc, throwable)
                    );
        } catch (Exception e) {
            rc.fail(400, new IllegalArgumentException("Invalid JSON payload"));
        }
    }

    private void getAllShares(RoutingContext rc) {
        int page = Integer.parseInt(rc.request().getParam("page", "1"));
        int size = Integer.parseInt(rc.request().getParam("size", "10"));

        getContextUser(rc, false, true)
                .chain(user -> Uni.combine().all().unis(
                        sharedSoundFragmentService.getAllAdminCount(),
                        sharedSoundFragmentService.getAllAdmin(size, (page - 1) * size)
                ).asTuple().map(tuple -> {
                    ViewPage viewPage = new ViewPage();
                    View<com.semantyca.datanest.dto.sharing.ShareAdminDTO> dtoEntries = new View<>(tuple.getItem2(),
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

    private void getShareById(RoutingContext rc) {
        UUID id = UUID.fromString(rc.pathParam("id"));
        getContextUser(rc, false, true)
                .chain(user -> sharedSoundFragmentService.getByIdAdmin(id))
                .subscribe().with(
                        dto -> rc.response()
                                .setStatusCode(200)
                                .putHeader("Content-Type", "application/json")
                                .end(JsonObject.mapFrom(dto).encode()),
                        t -> handleFailure(rc, t)
                );
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
