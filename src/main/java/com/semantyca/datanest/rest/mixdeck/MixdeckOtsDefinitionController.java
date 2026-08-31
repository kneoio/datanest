package com.semantyca.datanest.rest.mixdeck;

import com.semantyca.core.controller.AbstractSecuredController;
import com.semantyca.core.dto.actions.ActionBox;
import com.semantyca.core.dto.cnst.PayloadType;
import com.semantyca.core.dto.form.FormPage;
import com.semantyca.core.dto.view.View;
import com.semantyca.core.dto.view.ViewPage;
import com.semantyca.core.model.user.IUser;
import com.semantyca.core.service.UserService;
import com.semantyca.core.util.ProblemDetailsUtil;
import com.semantyca.core.util.RuntimeUtil;
import com.semantyca.datanest.dto.brand.mixdeck.OtsDefinitionMixdeckDTO;
import com.semantyca.datanest.util.DocumentIds;
import com.semantyca.datanest.service.BrandService;
import com.semantyca.datanest.service.OtsDefinitionService;
import com.semantyca.mixpla.model.filter.OtsDefinitionFilter;
import com.semantyca.mixpla.model.stream.OtsDefinition;
import io.smallrye.mutiny.Uni;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
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
public class MixdeckOtsDefinitionController extends AbstractSecuredController<OtsDefinition, OtsDefinitionMixdeckDTO> {
    private final OtsDefinitionService service;
    private final BrandService brandService;
    private final Validator validator;

    public MixdeckOtsDefinitionController() {
        super(null);
        this.service = null;
        this.brandService = null;
        this.validator = null;
    }

    @Inject
    public MixdeckOtsDefinitionController(UserService userService, OtsDefinitionService service,
                                          BrandService brandService, Validator validator) {
        super(userService);
        this.service = service;
        this.brandService = brandService;
        this.validator = validator;
    }

    public void setupRoutes(Router router) {
        String path = "/datanest/public/ots-definitions";
        router.route(path + "*").handler(BodyHandler.create());
        router.route(HttpMethod.GET, path).handler(this::getAll);
        router.route(HttpMethod.POST, path + "/:slugName?").handler(this::upsertBySlugName);
        router.route(HttpMethod.DELETE, path + "/:slugName").handler(this::deleteBySlugName);
        router.route(HttpMethod.GET, path + "/:slugName").handler(this::getBySlugName);
    }

    private void getAll(RoutingContext rc) {
        int page = Integer.parseInt(rc.request().getParam("page", "1"));
        int size = Integer.parseInt(rc.request().getParam("size", "10"));

        getContextUser(rc, false, true)
                .chain(user -> parseFilterDTO(rc, user)
                        .chain(filter -> {
                            assert service != null;
                            return Uni.combine().all().unis(
                                    service.getAllCount(user, filter),
                                    service.getAllMixdeckDTO(size, (page - 1) * size, user, filter)
                            ).asTuple();
                        }))
                .map(tuple -> {
                    ViewPage viewPage = new ViewPage();
                    View<OtsDefinitionMixdeckDTO> dtoEntries = new View<>(tuple.getItem2(),
                            tuple.getItem1(), page,
                            RuntimeUtil.countMaxPage(tuple.getItem1(), size),
                            size);
                    viewPage.addPayload(PayloadType.VIEW_DATA, dtoEntries);
                    viewPage.addPayload(PayloadType.CONTEXT_ACTIONS, new ActionBox());
                    return viewPage;
                })
                .subscribe().with(
                        viewPage -> rc.response()
                                .setStatusCode(200)
                                .putHeader("Content-Type", "application/json")
                                .end(io.vertx.core.json.Json.encode(viewPage)),
                        rc::fail
                );
    }

    private Uni<OtsDefinitionFilter> parseFilterDTO(RoutingContext rc, IUser user) {
        String filterParam = rc.request().getParam("filter");
        if (filterParam == null || filterParam.trim().isEmpty()) {
            return Uni.createFrom().nullItem();
        }

        OtsDefinitionFilter filter = new OtsDefinitionFilter();
        boolean any = false;
        try {
            JsonObject json = new JsonObject(filterParam);

            String brandSlug = json.getString("brandSlug");
            if (brandSlug != null && !brandSlug.trim().isEmpty()) {
                any = true;
                assert brandService != null;
                return brandService.getBySlugNameForUser(brandSlug.trim(), user)
                        .map(brand -> {
                            filter.setBrandId(brand.getId());
                            filter.setActivated(true);
                            applySearchAndActivated(json, filter);
                            return filter;
                        });
            }

            if (applySearchAndActivated(json, filter)) {
                any = true;
            }
        } catch (Exception e) {
            return Uni.createFrom().failure(
                    new IllegalArgumentException("Invalid filter JSON format: " + e.getMessage(), e));
        }

        return Uni.createFrom().item(any ? filter : null);
    }

    private boolean applySearchAndActivated(JsonObject json, OtsDefinitionFilter filter) {
        boolean any = false;
        String searchTerm = json.getString("searchTerm");
        if (searchTerm != null && !searchTerm.trim().isEmpty()) {
            filter.setSearchTerm(searchTerm.trim());
            any = true;
        }
        if (json.containsKey("activated")) {
            filter.setActivated(json.getBoolean("activated", false));
            any = true;
        } else if (any) {
            filter.setActivated(true);
        }
        return any;
    }

    private void getBySlugName(RoutingContext rc) {
        String slugName = rc.pathParam("slugName");

        getContextUser(rc, false, true)
                .chain(user -> {
                    assert service != null;
                    if ("new".equalsIgnoreCase(slugName)) {
                        String scriptSlug = rc.request().getParam("scriptSlug");
                        if (scriptSlug == null || scriptSlug.isBlank()) {
                            return Uni.createFrom().item(new OtsDefinitionMixdeckDTO());
                        }
                        return service.getNewMixdeckDTO(scriptSlug, user);
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

            OtsDefinitionMixdeckDTO dto = rc.body().asJsonObject().mapTo(OtsDefinitionMixdeckDTO.class);
            String slugName = rc.pathParam("slugName");

            Set<ConstraintViolation<OtsDefinitionMixdeckDTO>> violations = validator.validate(dto);
            if (violations != null && !violations.isEmpty()) {
                Map<String, List<String>> fieldErrors = new HashMap<>();
                for (ConstraintViolation<OtsDefinitionMixdeckDTO> v : violations) {
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
                        return service.upsertMixdeck(slugName, dto, user);
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
                    return service.deleteBySlug(slugName, user);
                })
                .subscribe().with(
                        count -> rc.response().setStatusCode(count > 0 ? 204 : 404).end(),
                        rc::fail
                );
    }
}
