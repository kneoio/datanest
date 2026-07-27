package com.semantyca.datanest.rest.mixdeck;

import com.semantyca.core.controller.AbstractSecuredController;
import com.semantyca.core.dto.cnst.PayloadType;
import com.semantyca.core.dto.view.View;
import com.semantyca.core.dto.view.ViewPage;
import com.semantyca.core.service.UserService;
import com.semantyca.core.util.RuntimeUtil;
import com.semantyca.datanest.dto.sharing.ReceivedSharePublicDTO;
import com.semantyca.datanest.dto.sharing.ShareDTO;
import com.semantyca.datanest.service.soundfragment.SharedSoundFragmentService;
import com.semantyca.mixpla.model.soundfragment.SharedSoundFragment;
import io.smallrye.mutiny.Uni;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class MixdeckSharedSoundFragmentController extends AbstractSecuredController<SharedSoundFragment, ShareDTO> {

    private final SharedSoundFragmentService sharedSoundFragmentService;

    public MixdeckSharedSoundFragmentController() {
        super(null);
        this.sharedSoundFragmentService = null;
    }

    @Inject
    public MixdeckSharedSoundFragmentController(UserService userService, SharedSoundFragmentService sharedSoundFragmentService) {
        super(userService);
        this.sharedSoundFragmentService = sharedSoundFragmentService;
    }

    public void setupRoutes(Router router) {
        router.route(HttpMethod.GET, "/datanest/public/shared-sound-fragments/received").handler(this::getReceived);
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

    protected void handleFailure(RoutingContext rc, Throwable throwable) {
        if (throwable instanceof IllegalStateException
                || throwable instanceof IllegalArgumentException) {
            rc.fail(401, throwable);
        } else {
            rc.fail(throwable);
        }
    }
}
