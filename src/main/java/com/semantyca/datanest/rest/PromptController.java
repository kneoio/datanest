package com.semantyca.datanest.rest;

import com.semantyca.core.controller.AbstractSecuredController;
import com.semantyca.core.dto.actions.ActionBox;
import com.semantyca.core.dto.cnst.PayloadType;
import com.semantyca.core.dto.form.FormPage;
import com.semantyca.core.dto.view.View;
import com.semantyca.core.dto.view.ViewPage;
import com.semantyca.core.model.cnst.LanguageCode;
import com.semantyca.core.model.cnst.LanguageTag;
import com.semantyca.core.service.UserService;
import com.semantyca.core.util.ProblemDetailsUtil;
import com.semantyca.core.util.RuntimeUtil;
import com.semantyca.datanest.dto.PromptDTO;
import com.semantyca.datanest.dto.agentrest.MasterPromptTranslateReqDTO;
import com.semantyca.datanest.dto.agentrest.TranslateReqDTO;
import com.semantyca.datanest.service.PromptService;
import com.semantyca.datanest.service.TranslateService;
import com.semantyca.datanest.service.prompt.MasterPromptTranslateAnthropicService;
import com.semantyca.mixpla.model.Prompt;
import com.semantyca.mixpla.model.cnst.PromptType;
import com.semantyca.mixpla.model.filter.PromptFilter;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple2;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

@ApplicationScoped
public class PromptController extends AbstractSecuredController<Prompt, PromptDTO> {
    private static final Logger LOGGER = LoggerFactory.getLogger(PromptController.class);

    private PromptService service;
    private Validator validator;

    private TranslateService translateService;
    private MasterPromptTranslateAnthropicService masterPromptTranslateAnthropicService;

    public PromptController() {
        super(null);
    }

    @Inject
    public PromptController(UserService userService,
                            PromptService service,
                            Validator validator,
                            TranslateService translateService,
                            MasterPromptTranslateAnthropicService masterPromptTranslateAnthropicService) {
        super(userService);
        this.service = service;
        this.validator = validator;
        this.translateService = translateService;
        this.masterPromptTranslateAnthropicService = masterPromptTranslateAnthropicService;
    }

    public void setupRoutes(Router router) {
        String path = "/datanest/prompts";
        router.route(path + "*").handler(BodyHandler.create());
        router.post(path + "/translate/start").handler(this::translateStart);
        router.get(path + "/translate/stream").handler(this::translateStream);
        router.get(path).handler(this::getAll);
        router.get(path + "/grouped").handler(this::getAllGrouped);
        router.post(path + "/test").handler(this::test);
        router.get(path + "/:id").handler(this::getById);
        router.post(path).handler(this::upsert);
        router.post(path + "/:id").handler(this::upsert);
        router.delete(path + "/:id").handler(this::delete);
        router.get(path + "/:id/access").handler(this::getDocumentAccess);
    }

    private void getAll(RoutingContext rc) {
        int page = Integer.parseInt(rc.request().getParam("page", "1"));
        int size = Integer.parseInt(rc.request().getParam("size", "10"));
        PromptFilter filter = parseFilterDTO(rc);

        getContextUser(rc, false, true)
                .chain(user -> Uni.combine().all().unis(
                        service.getAllCount(user, filter),
                        service.getAllDTO(size, (page - 1) * size, user, filter)
                ).asTuple().map(tuple -> {
                    ViewPage viewPage = new ViewPage();
                    View<PromptDTO> dtoEntries = new View<>(tuple.getItem2(),
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
                        throwable -> {
                            LOGGER.error("Failed to get all prompts", throwable);
                            rc.fail(throwable);
                        }
                );
    }

    private void getAllGrouped(RoutingContext rc) {
        int page = Integer.parseInt(rc.request().getParam("page", "1"));
        int size = Integer.parseInt(rc.request().getParam("size", "10"));
        PromptFilter filter = parseFilterDTO(rc);

        getContextUser(rc, false, true)
                .chain(user -> Uni.combine().all().unis(
                        service.getAllMastersCount(user, filter),
                        service.getAllGroupedDTO(size, (page - 1) * size, user, filter)
                ).asTuple().map(tuple -> {
                    ViewPage viewPage = new ViewPage();
                    View<PromptDTO> dtoEntries = new View<>(tuple.getItem2(),
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
                        throwable -> {
                            LOGGER.error("Failed to get grouped prompts", throwable);
                            rc.fail(throwable);
                        }
                );
    }

    private PromptFilter parseFilterDTO(RoutingContext rc) {
        String filterParam = rc.request().getParam("filter");
        if (filterParam == null || filterParam.isBlank()) {
            return null;
        }

        PromptFilter dto = new PromptFilter();
        boolean any = false;

        try {
            LOGGER.debug("Parsing filter parameter: {}", filterParam);
            JsonObject json = new JsonObject(filterParam);

            if (json.containsKey("languageTag")) {
                dto.setLanguageTag(LanguageTag.fromTag(json.getString("languageTag")));
                any = true;
            }

            if (json.containsKey("promptType")) {
                dto.setPromptType(PromptType.valueOf(json.getString("promptType")));
                any = true;
            }

            if (json.containsKey("enabled")) {
                dto.setEnabled(json.getBoolean("enabled"));
                any = true;
            }

            if (json.containsKey("master")) {
                dto.setMaster(json.getBoolean("master"));
                any = true;
            }

            if (json.containsKey("locked")) {
                dto.setLocked(json.getBoolean("locked"));
                any = true;
            }

            if (json.containsKey("activated")) {
                dto.setActivated(json.getBoolean("activated"));
                any = true;
            }

            return any ? dto : null;

        } catch (Exception e) {
            LOGGER.error("Error parsing filter parameters: '{}', error: {}", filterParam, e.getMessage(), e);
            return null;
        }
    }

    private void getById(RoutingContext rc) {
        String id = rc.pathParam("id");
        LanguageCode languageCode = LanguageCode.valueOf(rc.request().getParam("lang", LanguageCode.en.name()));

        getContextUser(rc, false, true)
                .chain(user -> {
                    if ("new".equalsIgnoreCase(id)) {
                        PromptDTO dto = new PromptDTO();
                        return Uni.createFrom().item(Tuple2.of(dto, user));
                    }
                    return service.getDTO(UUID.fromString(id), user, languageCode)
                            .map(doc -> Tuple2.of(doc, user));
                })
                .subscribe().with(
                        tuple -> {
                            PromptDTO doc = tuple.getItem1();
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

    private void upsert(RoutingContext rc) {
        try {
            if (!validateJsonBody(rc)) return;

            PromptDTO dto = rc.body().asJsonObject().mapTo(PromptDTO.class);
            String id = rc.pathParam("id");

            Set<ConstraintViolation<PromptDTO>> violations = validator.validate(dto);
            if (violations != null && !violations.isEmpty()) {
                Map<String, List<String>> fieldErrors = new HashMap<>();
                for (ConstraintViolation<PromptDTO> v : violations) {
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
                    .chain(user -> service.upsert(id, dto, user))
                    .subscribe().with(
                            doc -> sendUpsertResponse(rc, doc, id),
                            throwable -> handleUpsertFailure(rc, throwable)
                    );

        } catch (Exception e) {
            if (e instanceof IllegalArgumentException) {
                rc.fail(400, e);
            } else {
                rc.fail(400, new IllegalArgumentException("Invalid JSON payload"));
            }
        }
    }

    private void test(RoutingContext rc) {
        try {
            if (!validateJsonBody(rc)) return;

            MasterPromptTranslateReqDTO dto = rc.body().asJsonObject().mapTo(MasterPromptTranslateReqDTO.class);
            if (!validateDTO(rc, dto, validator)) return;

            getContextUser(rc, false, true)
                    .chain(ignoredUser -> masterPromptTranslateAnthropicService.translateMasterPrompt(dto))
                    .subscribe().with(
                            response -> rc.response()
                                    .setStatusCode(200)
                                    .putHeader("Content-Type", "application/json")
                                    .end(io.vertx.core.json.Json.encode(response)),
                            rc::fail
                    );

        } catch (Exception e) {
            rc.fail(400, new IllegalArgumentException("Invalid request: " + e.getMessage()));
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

    private void translateStart(RoutingContext rc) {
        try {
            String jobId = rc.request().getParam("jobId");
            if (jobId == null || jobId.isBlank()) {
                rc.fail(400, new IllegalArgumentException("jobId is required"));
                return;
            }

            JsonArray array = rc.body().asJsonArray();
            if (array == null) {
                rc.fail(400, new IllegalArgumentException("Invalid request: expected a JSON array"));
                return;
            }
            List<TranslateReqDTO> dtos = new ArrayList<>();
            for (int i = 0; i < array.size(); i++) {
                JsonObject obj = array.getJsonObject(i);
                dtos.add(obj.mapTo(TranslateReqDTO.class));
            }

            for (TranslateReqDTO dto : dtos) {
                Set<ConstraintViolation<TranslateReqDTO>> violations = validator.validate(dto);
                if (violations != null && !violations.isEmpty()) {
                    Map<String, List<String>> fieldErrors = new HashMap<>();
                    for (ConstraintViolation<TranslateReqDTO> v : violations) {
                        String field = v.getPropertyPath().toString();
                        fieldErrors.computeIfAbsent(field, k -> new ArrayList<>()).add(v.getMessage());
                    }
                    String detail = fieldErrors.entrySet().stream()
                            .flatMap(e -> e.getValue().stream().map(msg -> e.getKey() + ": " + msg))
                            .collect(Collectors.joining(", "));
                    ProblemDetailsUtil.respondValidationError(rc, detail, fieldErrors);
                    return;
                }
            }

            getContextUser(rc, false, true)
                    .subscribe().with(user -> {
                        translateService.startJobForPrompts(jobId, dtos, user);
                        rc.response()
                                .setStatusCode(202)
                                .putHeader("Content-Type", "application/json")
                                .end(new JsonObject().put("jobId", jobId).encode());
                    }, rc::fail);

        } catch (Exception e) {
            rc.fail(400, new IllegalArgumentException("Invalid request: " + e.getMessage()));
        }
    }

    private void translateStream(RoutingContext rc) {
        String jobId = rc.request().getParam("jobId");
        if (jobId == null || jobId.isBlank()) {
            rc.fail(400, new IllegalArgumentException("jobId is required"));
            return;
        }

        var resp = rc.response();
        resp.setChunked(true);
        resp.putHeader("Content-Type", "text/event-stream");
        resp.putHeader("Cache-Control", "no-cache");
        resp.putHeader("Connection", "keep-alive");

        java.util.function.Consumer<TranslateService.SseEvent> consumer = ev -> {
            try {
                StringBuilder sb = new StringBuilder();
                sb.append("event: ").append(ev.type()).append('\n');
                sb.append("data: ").append(ev.data() != null ? ev.data().encode() : "{}").append('\n').append('\n');
                resp.write(sb.toString());
            } catch (Exception ignored) { }
        };

        translateService.subscribe(jobId, consumer);

        // Unsubscribe when client disconnects
        resp.exceptionHandler(t -> translateService.unsubscribe(jobId, consumer));
        resp.endHandler(v -> translateService.unsubscribe(jobId, consumer));
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

}

