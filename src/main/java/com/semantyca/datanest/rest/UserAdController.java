package com.semantyca.datanest.rest;

import com.semantyca.core.controller.AbstractSecuredController;
import com.semantyca.core.dto.actions.ActionBox;
import com.semantyca.core.dto.cnst.PayloadType;
import com.semantyca.core.dto.form.FormPage;
import com.semantyca.core.dto.view.View;
import com.semantyca.core.dto.view.ViewPage;
import com.semantyca.core.model.cnst.LanguageCode;
import com.semantyca.core.service.UserService;
import com.semantyca.core.util.RuntimeUtil;
import com.semantyca.datanest.dto.UserAdDTO;
import com.semantyca.datanest.service.UserAdService;
import com.semantyca.mixpla.model.UserAd;
import com.semantyca.mixpla.model.filter.UserAdFilter;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple2;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.validation.Validator;

import java.util.UUID;

@ApplicationScoped
public class UserAdController extends AbstractSecuredController<UserAd, UserAdDTO> {
    private UserAdService service;
    private Validator validator;

    public UserAdController() {
        super(null);
    }

    @Inject
    public UserAdController(UserService userService, UserAdService service, Validator validator) {
        super(userService);
        this.service = service;
        this.validator = validator;
    }

    public void setupRoutes(Router router) {
        String path = "/datanest/user-ads";
        router.route(path + "*").handler(BodyHandler.create());
        router.get(path).handler(this::getAll);
        router.get(path + "/:id").handler(this::getById);
        router.post(path + "/:id").handler(this::upsert);
        router.delete(path + "/:id").handler(this::delete);
    }

    private void getAll(RoutingContext rc) {
        int page = Integer.parseInt(rc.request().getParam("page", "1"));
        int size = Integer.parseInt(rc.request().getParam("size", "10"));
        UserAdFilter filter = parseFilter(rc);

        getContextUser(rc, false, true)
                .chain(user -> Uni.combine().all().unis(
                        service.getAllCount(user, filter),
                        service.getAll(size, (page - 1) * size, user, filter)
                ).asTuple().map(tuple -> {
                    ViewPage viewPage = new ViewPage();
                    View<UserAdDTO> dtoEntries = new View<>(tuple.getItem2(),
                            tuple.getItem1(), page,
                            RuntimeUtil.countMaxPage(tuple.getItem1(), size),
                            size);
                    viewPage.addPayload(PayloadType.VIEW_DATA, dtoEntries);
                    viewPage.addPayload(PayloadType.CONTEXT_ACTIONS, new ActionBox());
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
        String id = rc.pathParam("id");
        LanguageCode languageCode = LanguageCode.valueOf(rc.request().getParam("lang", LanguageCode.en.name()));

        getContextUser(rc, false, true)
                .chain(user -> {
                    if ("new".equals(id)) {
                        UserAdDTO dto = new UserAdDTO();
                        return Uni.createFrom().item(Tuple2.of(dto, user));
                    }
                    return service.getDTO(UUID.fromString(id), user, languageCode)
                            .map(doc -> Tuple2.of(doc, user));
                })
                .subscribe().with(
                        tuple -> {
                            FormPage page = new FormPage();
                            page.addPayload(PayloadType.DOC_DATA, tuple.getItem1());
                            page.addPayload(PayloadType.CONTEXT_ACTIONS, new ActionBox());
                            rc.response()
                                    .setStatusCode(200)
                                    .putHeader("Content-Type", "application/json")
                                    .end(io.vertx.core.json.Json.encode(page));
                        },
                        rc::fail
                );
    }

    private void upsert(RoutingContext rc) {
        try {
            if (!validateJsonBody(rc)) return;
            String id = rc.pathParam("id");
            UserAdDTO dto = rc.body().asJsonObject().mapTo(UserAdDTO.class);
            if (!validateDTO(rc, dto, validator)) return;
            getContextUser(rc, false, true)
                    .chain(user -> service.upsert(id, dto, user))
                    .subscribe().with(
                            doc -> sendUpsertResponse(rc, doc, id),
                            throwable -> handleUpsertFailure(rc, throwable)
                    );
        } catch (Exception e) {
            rc.fail(400, new IllegalArgumentException("Invalid JSON payload"));
        }
    }

    private void delete(RoutingContext rc) {
        String id = rc.pathParam("id");
        getContextUser(rc, false, true)
                .chain(user -> service.delete(id, user))
                .subscribe().with(
                        count -> rc.response().setStatusCode(count > 0 ? 204 : 404).end(),
                        rc::fail
                );
    }

    private UserAdFilter parseFilter(RoutingContext rc) {
        String filterParam = rc.request().getParam("filter");
        if (filterParam == null || filterParam.trim().isEmpty()) {
            return null;
        }
        UserAdFilter filter = new UserAdFilter();
        boolean any = false;
        try {
            JsonObject json = new JsonObject(filterParam);
            String userId = json.getString("userId");
            if (userId != null && !userId.trim().isEmpty()) {
                try {
                    filter.setUserId(Long.parseLong(userId));
                    any = true;
                } catch (NumberFormatException ignored) {
                }
            }
            String searchTerm = json.getString("searchTerm");
            if (searchTerm != null && !searchTerm.trim().isEmpty()) {
                filter.setSearchTerm(searchTerm);
                any = true;
            }
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid filter JSON: " + e.getMessage(), e);
        }
        return any ? filter : null;
    }
}
