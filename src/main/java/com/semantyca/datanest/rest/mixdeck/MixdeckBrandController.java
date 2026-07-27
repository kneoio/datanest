package com.semantyca.datanest.rest.mixdeck;

import com.semantyca.core.controller.AbstractSecuredController;
import com.semantyca.core.dto.actions.ActionBox;
import com.semantyca.core.dto.cnst.PayloadType;
import com.semantyca.core.dto.view.View;
import com.semantyca.core.dto.view.ViewPage;
import com.semantyca.core.service.UserService;
import com.semantyca.core.util.RuntimeUtil;
import com.semantyca.datanest.dto.actionbars.SoundFragmentActionsFactory;
import com.semantyca.datanest.dto.brand.BrandPublicFlatDTO;
import com.semantyca.datanest.rest.BrandController;
import com.semantyca.datanest.service.BrandService;
import com.semantyca.mixpla.model.brand.Brand;
import com.semantyca.mixpla.model.filter.BrandFilter;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

@ApplicationScoped
public class MixdeckBrandController extends AbstractSecuredController<Brand, BrandPublicFlatDTO> {
    private static final Logger LOGGER = Logger.getLogger(MixdeckBrandController.class);

    private final BrandService service;

    public MixdeckBrandController() {
        super(null);
        this.service = null;
    }

    @Inject
    public MixdeckBrandController(UserService userService, BrandService service) {
        super(userService);
        this.service = service;
    }

    public void setupRoutes(Router router) {
        router.route(HttpMethod.GET, "/datanest/public/brands").handler(this::getAll);
    }

    private void getAll(RoutingContext rc) {
        int page = Integer.parseInt(rc.request().getParam("page", "1"));
        int size = Integer.parseInt(rc.request().getParam("size", "10"));
        BrandFilter filter = BrandController.parseFilterDTO(rc);

        getContextUser(rc, false, true)
                .chain(user -> service.getAllPublicFlatDTO(user, filter))
                .map(list -> {
                    ViewPage viewPage = new ViewPage();
                    View<BrandPublicFlatDTO> dtoEntries = new View<>(list,
                            list.size(), page,
                            RuntimeUtil.countMaxPage(list.size(), size),
                            size);
                    viewPage.addPayload(PayloadType.VIEW_DATA, dtoEntries);
                    ActionBox actions = SoundFragmentActionsFactory.getViewActions();
                    viewPage.addPayload(PayloadType.CONTEXT_ACTIONS, actions);
                    return viewPage;
                })
                .subscribe().with(
                        viewPage -> rc.response()
                                .setStatusCode(200)
                                .putHeader("Content-Type", "application/json")
                                .end(io.vertx.core.json.Json.encode(viewPage)),
                        throwable -> {
                            LOGGER.error("Failed to get all brands", throwable);
                            rc.fail(throwable);
                        }
                );
    }
}
