package com.semantyca.datanest.rest;

import com.semantyca.core.controller.AbstractSecuredController;
import com.semantyca.core.dto.actions.ActionBox;
import com.semantyca.core.dto.cnst.PayloadType;
import com.semantyca.core.dto.form.FormPage;
import com.semantyca.core.dto.view.View;
import com.semantyca.core.dto.view.ViewPage;
import com.semantyca.core.service.UserService;
import com.semantyca.core.util.ProblemDetailsUtil;
import com.semantyca.core.util.RuntimeUtil;
import com.semantyca.datanest.dto.ArtistDTO;
import com.semantyca.datanest.model.artist.Artist;
import com.semantyca.datanest.service.ArtistService;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.Json;
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
import java.util.UUID;
import java.util.stream.Collectors;

@ApplicationScoped
public class ArtistController extends AbstractSecuredController<Artist, ArtistDTO> {
    private ArtistService service;
    private Validator validator;

    public ArtistController() {
        super(null);
    }

    @Inject
    public ArtistController(UserService userService, ArtistService service, Validator validator) {
        super(userService);
        this.service = service;
        this.validator = validator;
    }

    public void setupRoutes(Router router) {
        String path = "/datanest/artists";
        router.route(path + "*").handler(BodyHandler.create());
        router.route(path + "*").handler(this::addHeaders);
        router.get(path).handler(this::get);
        router.get(path + "/:id").handler(this::getById);
        router.post(path + "/:id?").handler(this::upsert);
        router.delete(path + "/:id").handler(this::delete);
        router.get(path + "/:id/access").handler(this::getDocumentAccess);
    }

    private void get(RoutingContext rc) {
        int page = Integer.parseInt(rc.request().getParam("page", "1"));
        int size = Integer.parseInt(rc.request().getParam("size", "10"));

        getContextUser(rc, false, true)
                .chain(user -> Uni.combine().all().unis(
                        service.getAll(size, (page - 1) * size, user),
                        service.getAllCount(user)
                ).asTuple().map(tuple -> {
                    ViewPage viewPage = new ViewPage();
                    View<ArtistDTO> view = new View<>(tuple.getItem1(),
                            tuple.getItem2(), page,
                            RuntimeUtil.countMaxPage(tuple.getItem2(), size),
                            size);
                    viewPage.addPayload(PayloadType.VIEW_DATA, view);
                    return viewPage;
                }))
                .subscribe().with(
                        viewPage -> rc.response()
                                .setStatusCode(200)
                                .putHeader("Content-Type", "application/json")
                                .end(Json.encode(viewPage)),
                        rc::fail
                );
    }

    private void getById(RoutingContext rc) {
        String id = rc.pathParam("id");

        getContextUser(rc, false, true)
                .chain(user -> {
                    if ("new".equals(id)) {
                        return Uni.createFrom().item(new ArtistDTO());
                    }
                    return service.getDTO(UUID.fromString(id), user, resolveLanguage(rc));
                })
                .subscribe().with(
                        doc -> {
                            FormPage page = new FormPage();
                            page.addPayload(PayloadType.DOC_DATA, doc);
                            page.addPayload(PayloadType.CONTEXT_ACTIONS, new ActionBox());
                            rc.response()
                                    .setStatusCode(200)
                                    .putHeader("Content-Type", "application/json")
                                    .end(Json.encode(page));
                        },
                        rc::fail
                );
    }

    private void upsert(RoutingContext rc) {
        try {
            if (!validateJsonBody(rc)) return;

            ArtistDTO dto = rc.body().asJsonObject().mapTo(ArtistDTO.class);
            String id = rc.pathParam("id");

            Set<ConstraintViolation<ArtistDTO>> violations = validator.validate(dto);
            if (violations != null && !violations.isEmpty()) {
                Map<String, List<String>> fieldErrors = new HashMap<>();
                for (ConstraintViolation<ArtistDTO> v : violations) {
                    fieldErrors.computeIfAbsent(v.getPropertyPath().toString(), k -> new ArrayList<>())
                            .add(v.getMessage());
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
            rc.fail(400, new IllegalArgumentException("Invalid JSON payload"));
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
                            rc::fail
                    );
        } catch (IllegalArgumentException e) {
            rc.fail(400, new IllegalArgumentException("Invalid document ID format"));
        }
    }
}
