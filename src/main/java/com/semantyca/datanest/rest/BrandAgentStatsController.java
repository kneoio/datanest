package com.semantyca.datanest.rest;

import com.semantyca.core.controller.AbstractSecuredController;
import com.semantyca.core.dto.actions.ActionBox;
import com.semantyca.core.dto.cnst.PayloadType;
import com.semantyca.core.dto.form.FormPage;
import com.semantyca.core.dto.view.View;
import com.semantyca.core.dto.view.ViewPage;
import com.semantyca.core.service.UserService;
import com.semantyca.core.util.RuntimeUtil;
import com.semantyca.datanest.dto.BrandAgentStatsDTO;
import com.semantyca.datanest.service.BrandAgentStatsService;
import com.semantyca.mixpla.model.BrandAgentStats;
import com.semantyca.mixpla.model.cnst.StreamType;
import com.semantyca.mixpla.model.filter.BrandAgentStatsFilter;
import com.semantyca.officeframe.model.cnst.CountryCode;
import io.smallrye.mutiny.Uni;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class BrandAgentStatsController extends AbstractSecuredController<BrandAgentStats, BrandAgentStatsDTO> {
    private static final Logger LOGGER = LoggerFactory.getLogger(BrandAgentStatsController.class);

    private BrandAgentStatsService service;

    public BrandAgentStatsController() {
        super(null);
    }

    @Inject
    public BrandAgentStatsController(UserService userService, BrandAgentStatsService service) {
        super(userService);
        this.service = service;
    }

    public void setupRoutes(Router router) {
        String path = "/datanest/brand-agent-stats";
        router.route(path + "*").handler(BodyHandler.create());
        router.route(path + "*").handler(this::addHeaders);
        router.get(path).handler(this::getAll);
        router.get(path + "/:id").handler(this::getById);
    }

    private void getAll(RoutingContext rc) {
        int page = Integer.parseInt(rc.request().getParam("page", "1"));
        int size = Integer.parseInt(rc.request().getParam("size", "10"));
        BrandAgentStatsFilter filter = parseFilter(rc);

        getContextUser(rc, false, true)
                .chain(user -> Uni.combine().all().unis(
                        service.getAllCount(filter),
                        service.getAll(size, (page - 1) * size, filter)
                ).asTuple().map(tuple -> {
                    ViewPage viewPage = new ViewPage();
                    View<BrandAgentStatsDTO> dtoEntries = new View<>(tuple.getItem2(),
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
                        rc::fail
                );
    }

    private void getById(RoutingContext rc) {
        String idParam = rc.pathParam("id");

        getContextUser(rc, false, true)
                .chain(user -> service.getById(Integer.parseInt(idParam)))
                .subscribe().with(
                        dto -> {
                            FormPage page = new FormPage();
                            page.addPayload(PayloadType.DOC_DATA, dto);
                            page.addPayload(PayloadType.CONTEXT_ACTIONS, new ActionBox());
                            rc.response()
                                    .setStatusCode(200)
                                    .putHeader("Content-Type", "application/json")
                                    .end(io.vertx.core.json.Json.encode(page));
                        },
                        rc::fail
                );
    }

    private BrandAgentStatsFilter parseFilter(RoutingContext rc) {
        BrandAgentStatsFilter filter = new BrandAgentStatsFilter();
        boolean hasAny = false;

        String stationName = rc.request().getParam("stationName");
        if (stationName != null && !stationName.isBlank()) {
            filter.setStationName(stationName);
            hasAny = true;
        }

        String countryParam = rc.request().getParam("countryCode");
        if (countryParam != null && !countryParam.isBlank()) {
            try {
                filter.setCountryCode(CountryCode.valueOf(countryParam.trim()));
                hasAny = true;
            } catch (IllegalArgumentException e) {
                LOGGER.warn("Invalid countryCode: {}", countryParam);
            }
        }

        String streamTypeParam = rc.request().getParam("streamType");
        if (streamTypeParam != null && !streamTypeParam.isBlank()) {
            try {
                filter.setStreamType(StreamType.valueOf(streamTypeParam.trim()));
                hasAny = true;
            } catch (IllegalArgumentException e) {
                LOGGER.warn("Invalid streamType: {}", streamTypeParam);
            }
        }

        return hasAny ? filter : null;
    }
}
