package com.semantyca.datanest.rest;

import com.semantyca.core.controller.AbstractSecuredController;
import com.semantyca.core.dto.actions.ActionBox;
import com.semantyca.core.dto.cnst.PayloadType;
import com.semantyca.core.dto.form.FormPage;
import com.semantyca.core.dto.view.View;
import com.semantyca.core.dto.view.ViewPage;
import com.semantyca.core.service.UserService;
import com.semantyca.core.util.RuntimeUtil;
import com.semantyca.datanest.dto.ChatSummaryDTO;
import com.semantyca.datanest.model.ChatSummary;
import com.semantyca.datanest.service.ChatSummaryService;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple2;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.UUID;

@ApplicationScoped
public class ChatSummaryController extends AbstractSecuredController<ChatSummary, ChatSummaryDTO> {

    @Inject
    ChatSummaryService service;

    public ChatSummaryController() {
        super(null);
    }

    @Inject
    public ChatSummaryController(UserService userService, ChatSummaryService service) {
        super(userService);
        this.service = service;
    }

    public void setupRoutes(Router router) {
        String path = "/datanest/chat-summaries";
        router.route(path + "*").handler(BodyHandler.create());
        router.get(path).handler(this::getAll);
        router.get(path + "/:id").handler(this::getById);
        router.delete(path + "/:id").handler(this::delete);
    }

    private void getAll(RoutingContext rc) {
        int page = Integer.parseInt(rc.request().getParam("page", "1"));
        int size = Integer.parseInt(rc.request().getParam("size", "10"));
        String brandName = rc.request().getParam("brand");

        getContextUser(rc, false, true)
                .chain(user -> {
                    if (brandName != null && !brandName.isEmpty()) {
                        return Uni.combine().all().unis(
                                service.getCountByBrandName(brandName),
                                service.getByBrandName(brandName, size, (page - 1) * size)
                        ).asTuple().map(tuple -> {
                            ViewPage viewPage = new ViewPage();
                            View<ChatSummaryDTO> dtoEntries = new View<>(tuple.getItem2(),
                                    tuple.getItem1(), page,
                                    RuntimeUtil.countMaxPage(tuple.getItem1(), size),
                                    size);
                            viewPage.addPayload(PayloadType.VIEW_DATA, dtoEntries);
                            viewPage.addPayload(PayloadType.CONTEXT_ACTIONS, new ActionBox());
                            return viewPage;
                        });
                    } else {
                        return Uni.combine().all().unis(
                                service.getAllCount(),
                                service.getAll(size, (page - 1) * size)
                        ).asTuple().map(tuple -> {
                            ViewPage viewPage = new ViewPage();
                            View<ChatSummaryDTO> dtoEntries = new View<>(tuple.getItem2(),
                                    tuple.getItem1(), page,
                                    RuntimeUtil.countMaxPage(tuple.getItem1(), size),
                                    size);
                            viewPage.addPayload(PayloadType.VIEW_DATA, dtoEntries);
                            viewPage.addPayload(PayloadType.CONTEXT_ACTIONS, new ActionBox());
                            return viewPage;
                        });
                    }
                })
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

        getContextUser(rc, false, true)
                .chain(user -> {
                    if ("new".equals(id)) {
                        ChatSummaryDTO dto = new ChatSummaryDTO();
                        return Uni.createFrom().item(Tuple2.of(dto, user));
                    }
                    return service.getById(UUID.fromString(id))
                            .map(doc -> Tuple2.of(doc, user));
                })
                .subscribe().with(
                        tuple -> {
                            ChatSummaryDTO chatSummary = tuple.getItem1();
                            FormPage page = new FormPage();
                            page.addPayload(PayloadType.DOC_DATA, chatSummary);
                            page.addPayload(PayloadType.CONTEXT_ACTIONS, new ActionBox());
                            rc.response()
                                    .setStatusCode(200)
                                    .putHeader("Content-Type", "application/json")
                                    .end(io.vertx.core.json.Json.encode(page));
                        },
                        rc::fail
                );
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
}
