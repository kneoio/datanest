    package com.semantyca.datanest.rest;

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
    import com.semantyca.core.util.WebHelper;
    import com.semantyca.datanest.dto.actions.SoundFragmentActionsFactory;
    import com.semantyca.datanest.dto.radiostation.BrandDTO;
    import com.semantyca.datanest.service.BrandService;
    import com.semantyca.mixpla.model.brand.Brand;
    import com.semantyca.mixpla.model.filter.BrandFilter;
    import com.semantyca.officeframe.model.cnst.CountryCode;
    import io.smallrye.mutiny.Uni;
    import io.smallrye.mutiny.tuples.Tuple2;
    import io.vertx.core.json.JsonArray;
    import io.vertx.core.json.JsonObject;
    import io.vertx.ext.web.Router;
    import io.vertx.ext.web.RoutingContext;
    import io.vertx.ext.web.handler.BodyHandler;
    import jakarta.enterprise.context.ApplicationScoped;
    import jakarta.inject.Inject;
    import jakarta.validation.Validator;
    import org.slf4j.Logger;
    import org.slf4j.LoggerFactory;

    import java.util.*;
    import java.util.stream.Collectors;

    @ApplicationScoped
    public class BrandController extends AbstractSecuredController<Brand, BrandDTO> {
        private static final Logger LOGGER = LoggerFactory.getLogger(BrandController.class);

        private BrandService service;
        private Validator validator;

        public BrandController() {
            super(null);
        }

        @Inject
        public BrandController(UserService userService, BrandService service, Validator validator) {
            super(userService);
            this.service = service;
            this.validator = validator;
        }

        public void setupRoutes(Router router) {
            String path = "/datanest/brands";
            router.route(path + "*").handler(BodyHandler.create());
            router.get(path).handler(this::getAll);
            router.get(path + "/:id").handler(this::getById);
            router.post(path + "/:id?").handler(this::upsert);
            router.delete(path + "/:id").handler(this::delete);
            router.post(path + "/:id/close").handler(this::closeBrand);
            router.get(path + "/:id/access").handler(this::getDocumentAccess);
        }

        private void getAll(RoutingContext rc) {
            int page = Integer.parseInt(rc.request().getParam("page", "1"));
            int size = Integer.parseInt(rc.request().getParam("size", "10"));
            BrandFilter filter = parseFilterDTO(rc);

            getContextUser(rc, false, true)
                    .chain(user -> Uni.combine().all().unis(
                            service.getAllCount(user, filter),
                            service.getAllDTO(size, (page - 1) * size, user, filter)
                    ).asTuple().map(tuple -> {
                        ViewPage viewPage = new ViewPage();
                        View<BrandDTO> dtoEntries = new View<>(tuple.getItem2(),
                                tuple.getItem1(), page,
                                RuntimeUtil.countMaxPage(tuple.getItem1(), size),
                                size);
                        viewPage.addPayload(PayloadType.VIEW_DATA, dtoEntries);
                        ActionBox actions = SoundFragmentActionsFactory.getViewActions(user.getActivatedRoles());
                        viewPage.addPayload(PayloadType.CONTEXT_ACTIONS, actions);
                        return viewPage;
                    }))
                    .subscribe().with(
                            viewPage -> rc.response()
                                    .setStatusCode(200)
                                    .putHeader("Content-Type", "application/json")
                                    .end(io.vertx.core.json.Json.encode(viewPage)),
                            throwable -> {
                                LOGGER.error("Failed to get all radio stations", throwable);
                                rc.fail(throwable);
                            }
                    );
        }

        private void getById(RoutingContext rc) {
            String id = rc.pathParam("id");
            LanguageCode languageCode = LanguageCode.valueOf(rc.request().getParam("lang", LanguageCode.en.name()));

            getContextUser(rc, false, true)
                    .chain(user -> {
                        if ("new".equals(id)) {
                            BrandDTO dto = new BrandDTO();
                            dto.setLocalizedName(new EnumMap<>(LanguageCode.class));
                            dto.getLocalizedName().put(LanguageCode.en, "");
                            dto.setColor(WebHelper.generateRandomBrightColor());
                            dto.setBitRate(128000);
                            return Uni.createFrom().item(Tuple2.of(dto, user));
                        }
                        return service.getDTO(UUID.fromString(id), user, languageCode)
                                .map(doc -> Tuple2.of(doc, user));
                    })
                    .subscribe().with(
                            tuple -> {
                                BrandDTO doc = tuple.getItem1();
                                FormPage page = new FormPage();
                                page.addPayload(PayloadType.DOC_DATA, doc);
                                page.addPayload(PayloadType.CONTEXT_ACTIONS, new ActionBox());
                                rc.response()
                                        .setStatusCode(200)
                                        .putHeader("Content-Type", "application/json")
                                        .end(io.vertx.core.json.Json.encode(page));
                            },
                            throwable -> {
                                LOGGER.error("Failed to get radio station by id: {}", id, throwable);
                                rc.fail(throwable);
                            }
                    );
        }

        private void upsert(RoutingContext rc) {
            try {
                if (!validateJsonBody(rc)) {
                    return;
                }

                String id = rc.pathParam("id");
                BrandDTO dto = rc.body().asJsonObject().mapTo(BrandDTO.class);

                Set<jakarta.validation.ConstraintViolation<BrandDTO>> violations = validator.validate(dto);
                if (violations != null && !violations.isEmpty()) {
                    Map<String, List<String>> fieldErrors = new HashMap<>();
                    for (jakarta.validation.ConstraintViolation<BrandDTO> v : violations) {
                        String field = v.getPropertyPath().toString();
                        fieldErrors.computeIfAbsent(field, k -> new ArrayList<>()).add(v.getMessage());
                    }

                    String detail = fieldErrors.entrySet().stream()
                            .flatMap(e -> e.getValue().stream().map(msg -> e.getKey() + ": " + msg))
                            .collect(Collectors.joining(", "));

                    ProblemDetailsUtil.respondValidationError(rc, detail, fieldErrors);
                    return;
                }

                getContextUser(rc, false, true)
                        .chain(user -> service.upsert(id, dto, user, LanguageCode.en))
                        .subscribe().with(
                                doc -> rc.response().setStatusCode("new".equalsIgnoreCase(id) || id == null || id.isBlank() ? 201 : 200).putHeader("Content-Type", "application/json").end(io.vertx.core.json.Json.encode(doc)),
                                throwable -> {
                                    LOGGER.error("Failed to upsert radio station with id: {}", id, throwable);
                                    rc.fail(throwable);
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

        private void delete(RoutingContext rc) {
            String id = rc.pathParam("id");
            getContextUser(rc, false, true)
                    .chain(user -> service.archive(id, user))
                    .subscribe().with(
                            count -> rc.response().setStatusCode(count > 0 ? 204 : 404).end(),
                            rc::fail
                    );
        }

        private void closeBrand(RoutingContext rc) {
            String id = rc.pathParam("id");
            getContextUser(rc, false, true)
                    .chain(user -> service.closeBrand(id, user))
                    .subscribe().with(
                            count -> rc.response().setStatusCode(count > 0 ? 204 : 404).end(),
                            throwable -> {
                                LOGGER.error("Failed to close brand with id: {}", id, throwable);
                                rc.fail(throwable);
                            }
                    );
        }

        private void getDocumentAccess(RoutingContext rc) {
            String id = rc.pathParam("id");

            try {
                UUID documentId = UUID.fromString(id);

                getContextUser(rc, false, true)
                        .chain(user -> service.getDocumentAccess(documentId, user))
                        .subscribe().with(
                                accessList -> {
                                    JsonObject response = new JsonObject();
                                    response.put("documentId", id);
                                    response.put("accessList", accessList);
                                    rc.response()
                                            .setStatusCode(200)
                                            .putHeader("Content-Type", "application/json")
                                            .end(response.encode());
                                },
                                throwable -> {
                                    if (throwable instanceof IllegalArgumentException) {
                                        rc.fail(400, throwable);
                                    } else {
                                        rc.fail(500, throwable);
                                    }
                                }
                        );
            } catch (IllegalArgumentException e) {
                rc.fail(400, new IllegalArgumentException("Invalid document ID format"));
            }
        }

        private BrandFilter parseFilterDTO(RoutingContext rc) {
            String filterParam = rc.request().getParam("filter");
            if (filterParam == null || filterParam.trim().isEmpty()) {
                return null;
            }
            BrandFilter dto = new BrandFilter();
            boolean any = false;
            try {
                JsonObject json = new JsonObject(filterParam);

                JsonArray c = json.getJsonArray("countries");
                if (c != null && !c.isEmpty()) {
                    List<CountryCode> countries = new ArrayList<>();
                    for (Object o : c) {
                        if (o instanceof String s) {
                            try {
                                countries.add(CountryCode.valueOf(s));
                            } catch (IllegalArgumentException ignored) {
                            }
                        }
                    }
                    if (!countries.isEmpty()) {
                        dto.setCountries(countries);
                        any = true;
                    }
                }

                JsonArray l = json.getJsonArray("labels");
                if (l != null && !l.isEmpty()) {
                    List<UUID> labels = new ArrayList<>();
                    for (Object o : l) {
                        if (o instanceof String str) {
                            try {
                                labels.add(UUID.fromString(str));
                            } catch (IllegalArgumentException ignored) {
                            }
                        }
                    }
                    if (!labels.isEmpty()) {
                        dto.setLabels(labels);
                        any = true;
                    }
                }

                if (json.containsKey("publicBrand")) {
                    dto.setPublicBrand(json.getBoolean("publicBrand", false));
                    any = true;
                }

                if (json.containsKey("activated")) {
                    dto.setActivated(json.getBoolean("activated", false));
                    any = true;
                } else if (json.containsKey("filterActivated")) {
                    dto.setActivated(json.getBoolean("filterActivated", false));
                    any = true;
                }
            } catch (IllegalArgumentException e) {
                throw e;
            } catch (Exception e) {
                throw new IllegalArgumentException("Invalid filter JSON format: " + e.getMessage(), e);
            }
            return any ? dto : null;
        }

    }






