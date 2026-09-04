package com.semantyca.datanest.rest.mixdeck;

import com.semantyca.core.controller.AbstractSecuredController;
import com.semantyca.core.dto.actions.ActionBox;
import com.semantyca.core.dto.cnst.PayloadType;
import com.semantyca.core.dto.form.FormPage;
import com.semantyca.core.dto.view.View;
import com.semantyca.core.dto.view.ViewPage;
import com.semantyca.core.model.cnst.LanguageCode;
import com.semantyca.core.service.UserService;
import com.semantyca.core.util.ProblemDetailsUtil;
import com.semantyca.core.util.RuntimeUtil;
import com.semantyca.datanest.dto.brand.mixdeck.BrandListenerMixdeckDTO;
import com.semantyca.datanest.dto.brand.mixdeck.ListenerMixdeckDTO;
import com.semantyca.datanest.rest.ListenerController;
import com.semantyca.datanest.service.ListenerService;
import com.semantyca.datanest.util.DocumentIds;
import com.semantyca.mixpla.model.Listener;
import com.semantyca.mixpla.model.filter.ListenerFilter;
import io.smallrye.mutiny.Uni;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@ApplicationScoped
public class MixdeckListenerController extends AbstractSecuredController<Listener, ListenerMixdeckDTO> {
    private final ListenerService service;
    private final Validator validator;

    public MixdeckListenerController() {
        super(null);
        this.service = null;
        this.validator = null;
    }

    @Inject
    public MixdeckListenerController(UserService userService, ListenerService service, Validator validator) {
        super(userService);
        this.service = service;
        this.validator = validator;
    }

    public void setupRoutes(Router router) {
        String path = "/datanest/public/listeners";
        router.route(path + "*").handler(BodyHandler.create());
        router.route(HttpMethod.GET, path + "/available-listeners").handler(this::getBrandListeners);
        router.route(HttpMethod.POST, path + "/:slugName?").handler(this::upsertBySlugName);
        router.route(HttpMethod.DELETE, path + "/:slugName").handler(this::deleteBySlugName);
        router.route(HttpMethod.GET, path + "/:slugName").handler(this::getBySlugName);
    }

    private void getBrandListeners(RoutingContext rc) {
        String brandName = rc.request().getParam("brand");
        int page = Integer.parseInt(rc.request().getParam("page", "1"));
        int size = Integer.parseInt(rc.request().getParam("size", "10"));
        ListenerFilter filter = ListenerController.parseFilterDTO(rc);

        getContextUser(rc, false, true)
                .chain(user -> {
                    assert service != null;
                    return Uni.combine().all().unis(
                            service.getBrandListenersMixdeck(brandName, size, (page - 1) * size, user, filter),
                            service.getCountBrandListeners(brandName, user, filter)
                    ).asTuple().map(tuple -> {
                        ViewPage viewPage = new ViewPage();
                        View<BrandListenerMixdeckDTO> dtoEntries = new View<>(tuple.getItem1(),
                                tuple.getItem2(), page,
                                RuntimeUtil.countMaxPage(tuple.getItem2(), size),
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
                        rc::fail
                );
    }

    private void getBySlugName(RoutingContext rc) {
        String slugName = rc.pathParam("slugName");
        LanguageCode languageCode = LanguageCode.valueOf(rc.request().getParam("lang", LanguageCode.en.name()));

        getContextUser(rc, false, true)
                .chain(user -> {
                    assert service != null;
                    if ("new".equalsIgnoreCase(slugName)) {
                        return service.getNewMixdeckDTO(user, languageCode);
                    }
                    return service.getMixdeckDTOBySlug(slugName, user);
                })
                .subscribe().with(
                        doc -> {
                            FormPage page = new FormPage();
                            page.addPayload(PayloadType.DOC_DATA, doc);
                            page.addPayload(PayloadType.CONTEXT_ACTIONS, new ActionBox());
                            rc.response()
                                    .setStatusCode(200)
                                    .putHeader("Content-Type", "application/json")
                                    .end(io.vertx.core.json.Json.encode(page));
                        },
                        rc::fail
                );
    }

    private void upsertBySlugName(RoutingContext rc) {
        try {
            if (!validateJsonBody(rc)) {
                return;
            }

            ListenerMixdeckDTO dto = rc.body().asJsonObject().mapTo(ListenerMixdeckDTO.class);
            String slugName = rc.pathParam("slugName");
            String contextBrandSlug = parseNullableParam(rc, "contextBrandSlug");

            Set<ConstraintViolation<ListenerMixdeckDTO>> violations = validator.validate(dto);
            if (violations != null && !violations.isEmpty()) {
                Map<String, List<String>> fieldErrors = new HashMap<>();
                for (ConstraintViolation<ListenerMixdeckDTO> v : violations) {
                    String field = v.getPropertyPath().toString();
                    fieldErrors.computeIfAbsent(field, k -> new ArrayList<>()).add(v.getMessage());
                }
                String detail = fieldErrors.entrySet().stream()
                        .flatMap(e -> e.getValue().stream().map(msg -> e.getKey() + ": " + msg))
                        .collect(Collectors.joining(", "));
                ProblemDetailsUtil.respondValidationError(rc, detail, fieldErrors);
                return;
            }

            boolean isNew = DocumentIds.isNewDocumentId(slugName);

            getContextUser(rc, false, true)
                    .chain(user -> {
                        assert service != null;
                        return service.upsertMixdeck(slugName, dto, contextBrandSlug, user);
                    })
                    .subscribe().with(
                            doc -> rc.response()
                                    .setStatusCode(isNew ? 201 : 200)
                                    .putHeader("Content-Type", "application/json")
                                    .end(io.vertx.core.json.Json.encode(doc)),
                            throwable -> {
                                if (throwable instanceof IllegalArgumentException) {
                                    rc.fail(400, throwable);
                                } else {
                                    handleUpsertFailure(rc, throwable);
                                }
                            }
                    );
        } catch (Exception e) {
            if (e instanceof IllegalArgumentException) {
                rc.fail(400, e);
            } else {
                rc.fail(400, new IllegalArgumentException("Invalid JSON payload"));
            }
        }
    }

    private void deleteBySlugName(RoutingContext rc) {
        String slugName = rc.pathParam("slugName");
        getContextUser(rc, false, true)
                .chain(user -> {
                    assert service != null;
                    return service.archiveBySlug(slugName, user);
                })
                .subscribe().with(
                        count -> rc.response().setStatusCode(count > 0 ? 204 : 404).end(),
                        rc::fail
                );
    }

    private String parseNullableParam(RoutingContext rc, String paramName) {
        String value = rc.request().getParam(paramName);
        return (value == null || value.isEmpty()) ? null : value;
    }
}
