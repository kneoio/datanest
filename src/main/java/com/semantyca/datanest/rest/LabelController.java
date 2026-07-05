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
import com.semantyca.officeframe.dto.LabelDTO;
import com.semantyca.officeframe.dto.LabelFilterDTO;
import com.semantyca.officeframe.model.Label;
import com.semantyca.officeframe.service.LabelService;
import io.smallrye.mutiny.Uni;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.UUID;

/**
 * Mixdeck-facing read-only wrapper around officeframe's LabelService, scoped to shared (system)
 * labels plus the caller's own. Writes are not exposed here - personal label creation happens
 * server-side inside the owning entity's own save flow (e.g. SoundFragmentService), not via a
 * standalone REST call.
 */
@ApplicationScoped
public class LabelController extends AbstractSecuredController<Label, LabelDTO> {

    @Inject
    LabelService labelService;

    public LabelController() {
        super(null);
    }

    @Inject
    public LabelController(UserService userService, LabelService labelService) {
        super(userService);
        this.labelService = labelService;
    }

    public void setupRoutes(Router router) {
        String path = "/datanest/labels";

        router.route(path + "*").handler(this::addHeaders);
        router.route(HttpMethod.GET, path).handler(this::getAll);
        router.route(HttpMethod.GET, path + "/only/category/:category_name").handler(this::getOfCategory);
        router.route(HttpMethod.GET, path + "/:id").handler(this::get);
    }

    private void getAll(RoutingContext rc) {
        int page = Integer.parseInt(rc.request().getParam("page", "1"));
        int size = Integer.parseInt(rc.request().getParam("size", "10"));
        LanguageCode languageCode = resolveLanguage(rc);
        String category = rc.request().getParam("category");
        String search = rc.request().getParam("search");

        LabelFilterDTO filter = new LabelFilterDTO();
        if (category != null && !category.isBlank()) {
            filter.setCategory(category);
        }
        if (search != null && !search.isBlank()) {
            filter.setSearch(search);
        }

        getContextUser(rc)
                .chain(user -> Uni.combine().all().unis(
                        labelService.getAllCount(user, filter),
                        labelService.getAll(size, (page - 1) * size, filter, user, languageCode)
                ).asTuple().map(tuple -> {
                    ViewPage viewPage = new ViewPage();
                    viewPage.addPayload(PayloadType.CONTEXT_ACTIONS, new ActionBox());
                    View<LabelDTO> dtoEntries = new View<>(tuple.getItem2(),
                            tuple.getItem1(), page,
                            RuntimeUtil.countMaxPage(tuple.getItem1(), size),
                            size);
                    viewPage.addPayload(PayloadType.VIEW_DATA, dtoEntries);
                    return viewPage;
                }))
                .subscribe().with(
                        viewPage -> rc.response().setStatusCode(200).end(JsonObject.mapFrom(viewPage).encode()),
                        rc::fail
                );
    }

    private void getOfCategory(RoutingContext rc) {
        String categoryName = rc.pathParam("category_name");
        LanguageCode languageCode = resolveLanguage(rc);

        getContextUser(rc)
                .chain(user -> labelService.getOfCategory(categoryName, user, languageCode))
                .subscribe().with(
                        dtoList -> {
                            ViewPage viewPage = new ViewPage();
                            int pageNum = 1;
                            int pageSize = dtoList.size();
                            int count = dtoList.size();
                            View<LabelDTO> dtoEntries = new View<>(dtoList, count, pageNum, 1, pageSize);
                            viewPage.addPayload(PayloadType.VIEW_DATA, dtoEntries);
                            rc.response().setStatusCode(200).end(JsonObject.mapFrom(viewPage).encode());
                        },
                        rc::fail
                );
    }

    private void get(RoutingContext rc) {
        String id = rc.pathParam("id");
        LanguageCode languageCode = resolveLanguage(rc);

        getContextUser(rc)
                .chain(user -> labelService.getDTO(UUID.fromString(id), user, languageCode))
                .subscribe().with(
                        dto -> {
                            FormPage page = new FormPage();
                            page.addPayload(PayloadType.CONTEXT_ACTIONS, new ActionBox());
                            page.addPayload(PayloadType.DOC_DATA, dto);
                            rc.response().setStatusCode(200).end(JsonObject.mapFrom(page).encode());
                        },
                        rc::fail
                );
    }
}
