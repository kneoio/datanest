package com.semantyca.datanest.rest.mixdeck;

import com.semantyca.core.controller.AbstractSecuredController;
import com.semantyca.core.dto.cnst.PayloadType;
import com.semantyca.core.dto.view.View;
import com.semantyca.core.dto.view.ViewPage;
import com.semantyca.core.service.UserService;
import com.semantyca.core.util.RuntimeUtil;
import com.semantyca.datanest.dto.BrandListenerDTO;
import com.semantyca.datanest.dto.ListenerDTO;
import com.semantyca.datanest.rest.ListenerController;
import com.semantyca.datanest.service.ListenerService;
import com.semantyca.mixpla.model.Listener;
import com.semantyca.mixpla.model.filter.ListenerFilter;
import io.smallrye.mutiny.Uni;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class MixdeckListenerController extends AbstractSecuredController<Listener, ListenerDTO> {
    private final ListenerService service;

    public MixdeckListenerController() {
        super(null);
        this.service = null;
    }

    @Inject
    public MixdeckListenerController(UserService userService, ListenerService service) {
        super(userService);
        this.service = service;
    }

    public void setupRoutes(Router router) {
        router.route(HttpMethod.GET, "/datanest/public/listeners/available-listeners").handler(this::getBrandListeners);
    }

    private void getBrandListeners(RoutingContext rc) {
        String brandName = rc.request().getParam("brand");
        int page = Integer.parseInt(rc.request().getParam("page", "1"));
        int size = Integer.parseInt(rc.request().getParam("size", "10"));
        ListenerFilter filter = ListenerController.parseFilterDTO(rc);

        getContextUser(rc, false, true)
                .chain(user -> Uni.combine().all().unis(
                        service.getBrandListeners(brandName, size, (page - 1) * size, user, filter),
                        service.getCountBrandListeners(brandName, user, filter)
                ).asTuple().map(tuple -> {
                    ViewPage viewPage = new ViewPage();
                    View<BrandListenerDTO> dtoEntries = new View<>(tuple.getItem1(),
                            tuple.getItem2(), page,
                            RuntimeUtil.countMaxPage(tuple.getItem2(), size),
                            size);
                    viewPage.addPayload(PayloadType.VIEW_DATA, dtoEntries);
                    return viewPage;
                }))
                .subscribe().with(
                        viewPage -> rc.response()
                                .setStatusCode(200)
                                .putHeader("Content-Type", "application/json")
                                .end(io.vertx.core.json.Json.encode(viewPage)),
                        rc::fail
                );
    }
}
