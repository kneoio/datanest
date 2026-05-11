package com.semantyca.datanest.rest;

import com.semantyca.core.controller.AbstractSecuredController;
import com.semantyca.core.dto.actions.ActionBox;
import com.semantyca.core.dto.cnst.PayloadType;
import com.semantyca.core.dto.view.View;
import com.semantyca.core.dto.view.ViewPage;
import com.semantyca.core.model.cnst.ArchivedStatus;
import com.semantyca.core.model.cnst.LanguageCode;
import com.semantyca.core.repository.exception.UserNotFoundException;
import com.semantyca.core.service.UserService;
import com.semantyca.core.util.RuntimeUtil;
import com.semantyca.datanest.dto.SharedSoundFragmentPatchDTO;
import com.semantyca.datanest.dto.SoundFragmentDTO;
import com.semantyca.datanest.dto.actions.SoundFragmentActionsFactory;
import com.semantyca.datanest.service.soundfragment.SoundFragmentService;
import com.semantyca.mixpla.model.cnst.SourceType;
import com.semantyca.mixpla.model.filter.SoundFragmentFilter;
import io.smallrye.mutiny.Uni;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.List;
import java.util.UUID;

@ApplicationScoped
public class SharedSoundFragmentController extends AbstractSecuredController<Object, Object> {

    private final SoundFragmentService soundFragmentService;
    private final SoundFragmentController soundFragmentController;

    public SharedSoundFragmentController() {
        super(null);
        this.soundFragmentService = null;
        this.soundFragmentController = null;
    }

    @Inject
    public SharedSoundFragmentController(UserService userService,
                                         SoundFragmentService soundFragmentService,
                                         SoundFragmentController soundFragmentController) {
        super(userService);
        this.soundFragmentService = soundFragmentService;
        this.soundFragmentController = soundFragmentController;
    }

    public void setupRoutes(Router router) {
        String path = "/datanest/shared-sound-fragments";
        BodyHandler jsonBodyHandler = BodyHandler.create().setHandleFileUploads(false);
        router.route(HttpMethod.GET, path).handler(this::getMySharedFragments);
        router.route(HttpMethod.PATCH, path + "/:fragmentId").handler(jsonBodyHandler).handler(this::patchShares);
        router.route(HttpMethod.GET, "/datanest/soundfragments/pending-review").handler(this::getPendingReview);
    }

    private void getMySharedFragments(RoutingContext rc) {
        int page = Integer.parseInt(rc.request().getParam("page", "1"));
        int size = Integer.parseInt(rc.request().getParam("size", "10"));

        getContextUser(rc, false, true)
                .chain(user -> {
                    assert soundFragmentService != null;
                    return Uni.combine().all().unis(
                            soundFragmentService.getMySharedContributionsCount(user),
                            soundFragmentService.getMySharedContributionDTOs(size, (page - 1) * size, user)
                    ).asTuple().map(tuple -> {
                        ViewPage viewPage = new ViewPage();
                        View<SoundFragmentDTO> dtoEntries = new View<>(tuple.getItem2(),
                                tuple.getItem1(), page,
                                RuntimeUtil.countMaxPage(tuple.getItem1(), size),
                                size);
                        viewPage.addPayload(PayloadType.VIEW_DATA, dtoEntries);
                        ActionBox actions = SoundFragmentActionsFactory.getViewActions(user.getActivatedRoles());
                        viewPage.addPayload(PayloadType.CONTEXT_ACTIONS, actions);
                        return viewPage;
                    });
                })
                .subscribe().with(
                        viewPage -> rc.response()
                                .setStatusCode(200)
                                .putHeader("Content-Type", "application/json")
                                .end(io.vertx.core.json.Json.encode(viewPage)),
                        t -> handleFailure(rc, t)
                );
    }

    private void getPendingReview(RoutingContext rc) {
        int page = Integer.parseInt(rc.request().getParam("page", "1"));
        int size = Integer.parseInt(rc.request().getParam("size", "10"));
        assert soundFragmentController != null;
        SoundFragmentFilter filter = soundFragmentController.parseFilterDTO(rc, List.of(SourceType.CONTRIBUTION));

        getContextUser(rc, false, true)
                .chain(user -> {
                    assert soundFragmentService != null;
                    return Uni.combine().all().unis(
                            soundFragmentService.getAllCount(user, filter, ArchivedStatus.HIDDEN),
                            soundFragmentService.getAllDTO(size, (page - 1) * size, user, filter, ArchivedStatus.HIDDEN)
                    ).asTuple().map(tuple -> {
                        ViewPage viewPage = new ViewPage();
                        View<SoundFragmentDTO> dtoEntries = new View<>(tuple.getItem2(),
                                tuple.getItem1(), page,
                                RuntimeUtil.countMaxPage(tuple.getItem1(), size),
                                size);
                        viewPage.addPayload(PayloadType.VIEW_DATA, dtoEntries);
                        ActionBox actions = SoundFragmentActionsFactory.getViewActions(user.getActivatedRoles());
                        viewPage.addPayload(PayloadType.CONTEXT_ACTIONS, actions);
                        return viewPage;
                    });
                })
                .subscribe().with(
                        viewPage -> rc.response()
                                .setStatusCode(200)
                                .putHeader("Content-Type", "application/json")
                                .end(io.vertx.core.json.Json.encode(viewPage)),
                        t -> handleFailure(rc, t)
                );
    }

    private void patchShares(RoutingContext rc) {
        UUID fragmentId = UUID.fromString(rc.pathParam("fragmentId"));
        LanguageCode languageCode = LanguageCode.valueOf(rc.request().getParam("lang", LanguageCode.en.name()));
        SharedSoundFragmentPatchDTO patch = rc.body().asJsonObject().mapTo(SharedSoundFragmentPatchDTO.class);

        getContextUser(rc, false, true)
                .chain(user -> {
                    assert soundFragmentService != null;
                    return soundFragmentService.patchSharedContributionTargets(fragmentId, patch, user, languageCode);
                })
                .subscribe().with(
                        dto -> rc.response()
                                .setStatusCode(200)
                                .putHeader("Content-Type", "application/json")
                                .end(JsonObject.mapFrom(dto).encode()),
                        t -> handleFailure(rc, t)
                );
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
