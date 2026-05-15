package com.semantyca.datanest.rest;

import com.semantyca.core.controller.AbstractSecuredController;
import com.semantyca.core.dto.actions.ActionBox;
import com.semantyca.core.dto.cnst.PayloadType;
import com.semantyca.core.dto.view.View;
import com.semantyca.core.dto.view.ViewPage;
import com.semantyca.core.repository.exception.UserNotFoundException;
import com.semantyca.core.service.UserService;
import com.semantyca.core.util.RuntimeUtil;
import com.semantyca.datanest.dto.SharedSoundDTO;
import com.semantyca.datanest.dto.SharedSoundFragmentDTO;
import com.semantyca.datanest.dto.SharedSoundFragmentPatchDTO;
import com.semantyca.datanest.dto.SharedSoundFragmentPreviewDTO;
import com.semantyca.datanest.dto.actions.SoundFragmentActionsFactory;
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
public class SharedSoundFragmentController extends AbstractSecuredController<SharedSoundFragment, SharedSoundFragmentDTO> {

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
        router.route(HttpMethod.GET,    path + "/shared").handler(this::getMySharedFragments);
        router.route(HttpMethod.PATCH,  path + "/shared/:fragmentId").handler(jsonBodyHandler).handler(this::patchShares);
        router.route(HttpMethod.GET,    path + "/received").handler(this::getReceived);
        router.route(HttpMethod.GET,    path + "/received/:id").handler(this::getReceivedDoc);
        router.route(HttpMethod.DELETE, path + "/received/:id").handler(this::rejectShare);
        router.route(HttpMethod.GET,    path + "/shared/:id/access").handler(this::getDocumentAccess);
        //only for administrator app
        router.route(HttpMethod.GET,    "/datanest/shared-sound-fragments/:id/access").handler(this::getDocumentAccess);
    }

    private void getMySharedFragments(RoutingContext rc) {
        int page = Integer.parseInt(rc.request().getParam("page", "1"));
        int size = Integer.parseInt(rc.request().getParam("size", "10"));

        getContextUser(rc, false, true)
                .chain(user -> Uni.combine().all().unis(
                        sharedSoundFragmentService.getSharedCount(user),
                        sharedSoundFragmentService.getShared(size, (page - 1) * size, user)
                ).asTuple().map(tuple -> {
                    ViewPage viewPage = new ViewPage();
                    View<SharedSoundDTO> dtoEntries = new View<>(tuple.getItem2(),
                            tuple.getItem1(), page,
                            RuntimeUtil.countMaxPage(tuple.getItem1(), size),
                            size);
                    viewPage.addPayload(PayloadType.VIEW_DATA, dtoEntries);
                    ActionBox actions = SoundFragmentActionsFactory.getViewActions();
                    viewPage.addPayload(PayloadType.CONTEXT_ACTIONS, actions);
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

    private void getReceived(RoutingContext rc) {
        int page = Integer.parseInt(rc.request().getParam("page", "1"));
        int size = Integer.parseInt(rc.request().getParam("size", "10"));

        getContextUser(rc, false, true)
                .chain(user -> Uni.combine().all().unis(
                        sharedSoundFragmentService.getReceivedListCount(user),
                        sharedSoundFragmentService.getReceivedList(size, (page - 1) * size, user)
                ).asTuple().map(tuple -> {
                    ViewPage viewPage = new ViewPage();
                    View<SharedSoundFragmentPreviewDTO> dtoEntries = new View<>(tuple.getItem2(),
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

    private void rejectShare(RoutingContext rc) {
        UUID shareId = UUID.fromString(rc.pathParam("id"));
        getContextUser(rc, false, true)
                .chain(user -> sharedSoundFragmentService.rejectShareByReceiver(shareId, user))
                .subscribe().with(
                        count -> rc.response().setStatusCode(204).end(),
                        t -> handleFailure(rc, t)
                );
    }

    private void patchShares(RoutingContext rc) {
        try {
            if (!validateJsonBody(rc)) return;

            UUID fragmentId = UUID.fromString(rc.pathParam("fragmentId"));
            SharedSoundFragmentPatchDTO patch = rc.body().asJsonObject().mapTo(SharedSoundFragmentPatchDTO.class);
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
