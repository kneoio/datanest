package com.semantyca.datanest.rest.mixdeck;

import com.semantyca.core.controller.AbstractSecuredController;
import com.semantyca.core.dto.cnst.PayloadType;
import com.semantyca.core.dto.form.FormPage;
import com.semantyca.core.dto.view.View;
import com.semantyca.core.dto.view.ViewPage;
import com.semantyca.core.repository.exception.DocumentHasNotFoundException;
import com.semantyca.core.service.UserService;
import com.semantyca.core.util.RuntimeUtil;
import com.semantyca.datanest.dto.sharing.ReceivedSharePublicDTO;
import com.semantyca.datanest.dto.sharing.ShareDTO;
import com.semantyca.datanest.dto.sharing.SharePatchMixdeckDTO;
import com.semantyca.datanest.dto.sharing.ShareTargetBrandDTO;
import com.semantyca.datanest.service.BrandService;
import com.semantyca.datanest.service.soundfragment.SharedSoundFragmentService;
import com.semantyca.mixpla.model.soundfragment.SharedSoundFragment;
import io.smallrye.mutiny.Uni;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.validation.Validator;
import org.jboss.logging.Logger;

@ApplicationScoped
public class MixdeckSharedSoundFragmentController extends AbstractSecuredController<SharedSoundFragment, ShareDTO> {
    private static final Logger LOGGER = Logger.getLogger(MixdeckSharedSoundFragmentController.class);

    private final SharedSoundFragmentService sharedSoundFragmentService;
    private final BrandService brandService;
    private final Validator validator;

    public MixdeckSharedSoundFragmentController() {
        super(null);
        this.sharedSoundFragmentService = null;
        this.brandService = null;
        this.validator = null;
    }

    @Inject
    public MixdeckSharedSoundFragmentController(UserService userService,
                                                SharedSoundFragmentService sharedSoundFragmentService,
                                                BrandService brandService,
                                                Validator validator) {
        super(userService);
        this.sharedSoundFragmentService = sharedSoundFragmentService;
        this.brandService = brandService;
        this.validator = validator;
    }

    public void setupRoutes(Router router) {
        BodyHandler jsonBodyHandler = BodyHandler.create().setHandleFileUploads(false);
        router.route(HttpMethod.GET, "/datanest/public/shared-sound-fragments/received").handler(this::getReceived);
        router.route(HttpMethod.PATCH, "/datanest/public/shared-sound-fragments/received/:brandSlug/:fragmentSlug/accept")
                .handler(this::acceptShareByReceiver);
        router.route(HttpMethod.PATCH, "/datanest/public/shared-sound-fragments/received/:brandSlug/:fragmentSlug/reject")
                .handler(this::rejectShareByReceiver);
        router.route(HttpMethod.DELETE, "/datanest/public/shared-sound-fragments/received/:brandSlug/:fragmentSlug")
                .handler(this::archiveRejectedByReceiver);
        router.route(HttpMethod.GET, "/datanest/public/shared-sound-fragments/received/:brandSlug/:fragmentSlug")
                .handler(this::getReceivedDoc);
        router.route(HttpMethod.GET, "/datanest/public/shared-sound-fragments/discover").handler(this::getShareTargets);
        // slug = source station attributing the share; fragmentSlug = song. FE sends NO_BRAND when unassigned.
        router.route(HttpMethod.PATCH, "/datanest/public/shared-sound-fragments/shared/:slug/:fragmentSlug")
                .handler(jsonBodyHandler).handler(this::patchToShare);
    }

    private void patchToShare(RoutingContext rc) {
        try {
            if (!validateJsonBody(rc)) return;

            String slug = rc.pathParam("slug");
            String fragmentSlug = rc.pathParam("fragmentSlug");
            SharePatchMixdeckDTO patch = rc.body().asJsonObject().mapTo(SharePatchMixdeckDTO.class);
            if (!validateDTO(rc, patch, validator)) return;

            getContextUser(rc, false, true)
                    .chain(user -> {
                        assert sharedSoundFragmentService != null;
                        return sharedSoundFragmentService.patchShares(fragmentSlug, slug, patch, user)
                                .chain(() -> sharedSoundFragmentService.listShareDTO(fragmentSlug, user));
                    })
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

    private void getShareTargets(RoutingContext rc) {
        int page = Integer.parseInt(rc.request().getParam("page", "1"));
        int size = Integer.parseInt(rc.request().getParam("size", "10"));

        getContextUser(rc, false, true)
                .chain(user -> {
                    assert brandService != null;
                    return Uni.combine().all().unis(
                            brandService.getAllOpenForSubmissionCount(user),
                            brandService.getAllOpenForSubmissionShareTargets(size, (page - 1) * size, user)
                    ).asTuple().map(tuple -> {
                        ViewPage viewPage = new ViewPage();
                        View<ShareTargetBrandDTO> dtoEntries = new View<>(tuple.getItem2(),
                                tuple.getItem1(), page,
                                RuntimeUtil.countMaxPage(tuple.getItem1(), size),
                                size);
                        viewPage.addPayload(PayloadType.VIEW_DATA, dtoEntries);
                        return viewPage;
                    });
                })
                .subscribe().with(
                        viewPage -> rc.response()
                                .setStatusCode(200)
                                .putHeader("Content-Type", "application/json")
                                .end(io.vertx.core.json.Json.encode(viewPage)),
                        throwable -> {
                            LOGGER.error("Failed to get share-target brands", throwable);
                            rc.fail(throwable);
                        }
                );
    }

    private void getReceived(RoutingContext rc) {
        int page = Integer.parseInt(rc.request().getParam("page", "1"));
        int size = Integer.parseInt(rc.request().getParam("size", "10"));
        String search = rc.request().getParam("search");
        if (search != null && search.isBlank()) {
            search = null;
        }
        String searchParam = search;

        getContextUser(rc, false, true)
                .chain(user -> {
                    assert sharedSoundFragmentService != null;
                    return Uni.combine().all().unis(
                            sharedSoundFragmentService.getSharingPreviewCount(user, searchParam),
                            sharedSoundFragmentService.getPublicSharingPreviewList(size, (page - 1) * size, user, searchParam)
                    ).asTuple().map(tuple -> {
                        ViewPage viewPage = new ViewPage();
                        View<ReceivedSharePublicDTO> dtoEntries = new View<>(tuple.getItem2(),
                                tuple.getItem1(), page,
                                RuntimeUtil.countMaxPage(tuple.getItem1(), size),
                                size);
                        viewPage.addPayload(PayloadType.VIEW_DATA, dtoEntries);
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

    private void getReceivedDoc(RoutingContext rc) {
        String brandSlug = rc.pathParam("brandSlug");
        String fragmentSlug = rc.pathParam("fragmentSlug");
        getContextUser(rc, false, true)
                .chain(user -> {
                    assert sharedSoundFragmentService != null;
                    return sharedSoundFragmentService.getPublicByBrandAndFragmentSlug(brandSlug, fragmentSlug, user);
                })
                .subscribe().with(
                        dto -> {
                            FormPage page = new FormPage();
                            page.addPayload(PayloadType.DOC_DATA, dto);
                            rc.response()
                                    .setStatusCode(200)
                                    .putHeader("Content-Type", "application/json")
                                    .end(io.vertx.core.json.Json.encode(page));
                        },
                        t -> handleFailure(rc, t)
                );
    }

    private void acceptShareByReceiver(RoutingContext rc) {
        String brandSlug = rc.pathParam("brandSlug");
        String fragmentSlug = rc.pathParam("fragmentSlug");
        getContextUser(rc, false, true)
                .chain(user -> {
                    assert sharedSoundFragmentService != null;
                    return sharedSoundFragmentService.acceptShareByReceiver(brandSlug, fragmentSlug, user);
                })
                .subscribe().with(
                        count -> rc.response().setStatusCode(204).end(),
                        t -> handleFailure(rc, t)
                );
    }

    private void rejectShareByReceiver(RoutingContext rc) {
        String brandSlug = rc.pathParam("brandSlug");
        String fragmentSlug = rc.pathParam("fragmentSlug");
        getContextUser(rc, false, true)
                .chain(user -> {
                    assert sharedSoundFragmentService != null;
                    return sharedSoundFragmentService.rejectShareByReceiver(brandSlug, fragmentSlug, user);
                })
                .subscribe().with(
                        count -> rc.response().setStatusCode(204).end(),
                        t -> handleFailure(rc, t)
                );
    }

    private void archiveRejectedByReceiver(RoutingContext rc) {
        String brandSlug = rc.pathParam("brandSlug");
        String fragmentSlug = rc.pathParam("fragmentSlug");
        getContextUser(rc, false, true)
                .chain(user -> {
                    assert sharedSoundFragmentService != null;
                    return sharedSoundFragmentService.archiveRejectedShareByReceiver(brandSlug, fragmentSlug, user);
                })
                .subscribe().with(
                        count -> rc.response().setStatusCode(count > 0 ? 204 : 404).end(),
                        t -> handleFailure(rc, t)
                );
    }

    protected void handleFailure(RoutingContext rc, Throwable throwable) {
        if (throwable instanceof DocumentHasNotFoundException) {
            rc.fail(404, throwable);
        } else if (throwable instanceof IllegalStateException
                || throwable instanceof IllegalArgumentException) {
            rc.fail(401, throwable);
        } else {
            rc.fail(throwable);
        }
    }
}
