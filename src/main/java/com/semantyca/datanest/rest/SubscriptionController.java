package com.semantyca.datanest.rest;

import com.semantyca.core.controller.AbstractSecuredController;
import com.semantyca.core.model.SubscriptionProduct;
import com.semantyca.core.service.SubscriptionProductService;
import com.semantyca.core.service.UserService;
import com.semantyca.core.util.RuntimeUtil;
import com.semantyca.datanest.dto.subscription.UserSubscriptionProductStatusDTO;
import com.semantyca.datanest.dto.subscription.SubscriptionProductDTO;
import com.semantyca.datanest.dto.subscription.UserSubscriptionDTO;
import com.semantyca.datanest.repository.UserSubscriptionRepository;
import com.semantyca.mixpla.model.UserSubscription;
import io.smallrye.mutiny.Uni;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

@ApplicationScoped
public class SubscriptionController extends AbstractSecuredController<SubscriptionProduct, SubscriptionProductDTO> {

    private SubscriptionProductService productService;
    private UserSubscriptionRepository subscriptionRepository;
    private UserService userService;

    public SubscriptionController() {
        super(null);
    }

    @Inject
    public SubscriptionController(UserService userService,
                                   SubscriptionProductService productService,
                                   UserSubscriptionRepository subscriptionRepository) {
        super(userService);
        this.userService = userService;
        this.productService = productService;
        this.subscriptionRepository = subscriptionRepository;
    }

    public void setupRoutes(Router router) {
        String productPath = "/datanest/subscription-products";
        String subPath = "/datanest/user_subscriptions";
        BodyHandler jsonBodyHandler = BodyHandler.create().setHandleFileUploads(false);

        router.get("/datanest/subscriptions/products").handler(this::getProducts);

        router.route(productPath + "*").handler(requireRoles("admitp")).handler(this::addHeaders);
        router.route(HttpMethod.GET, productPath).handler(this::getAllProducts);
        router.route(HttpMethod.GET, productPath + "/:id").handler(this::getProduct);
        router.route(HttpMethod.POST, productPath + "/:id?").handler(jsonBodyHandler).handler(this::upsertProduct);
        router.route(HttpMethod.DELETE, productPath + "/:id").handler(this::deleteProduct);

        router.route(subPath + "*").handler(requireRoles("admitp")).handler(this::addHeaders);
        router.route(HttpMethod.GET, subPath).handler(this::getAll);
        router.route(HttpMethod.GET, subPath + "/:id").handler(this::get);
        router.route(HttpMethod.POST, subPath + "/:id?").handler(jsonBodyHandler).handler(this::upsert);
        router.route(HttpMethod.DELETE, subPath + "/:id").handler(this::delete);
    }

    private void getProducts(RoutingContext rc) {
        int page = Integer.parseInt(rc.request().getParam("page", "1"));
        int size = Integer.parseInt(rc.request().getParam("size", "100"));

        getContextUser(rc, false, true)
                .chain(user -> Uni.combine().all().unis(
                        productService.getAll(size, (page - 1) * size),
                        subscriptionRepository.findByUserId(user.getId())
                ).asTuple())
                .map(tuple -> {
                    Map<String, UserSubscription> subsByType = tuple.getItem2().stream()
                            .collect(Collectors.toMap(
                                    UserSubscription::getSubscriptionType,
                                    s -> s,
                                    (a, b) -> a.isActive() ? a : b
                            ));
                    return tuple.getItem1().stream()
                            .map(p -> UserSubscriptionProductStatusDTO.from(p, subsByType.get(p.getIdentifier())))
                            .collect(Collectors.toList());
                })
                .subscribe().with(
                        list -> rc.response()
                                .setStatusCode(200)
                                .putHeader("Content-Type", "application/json")
                                .end(Json.encode(list)),
                        rc::fail
                );
    }

    private void getAllProducts(RoutingContext rc) {
        int page = Integer.parseInt(rc.request().getParam("page", "1"));
        int size = Integer.parseInt(rc.request().getParam("size", "10"));

        getContextUser(rc)
                .chain(user -> Uni.combine().all().unis(
                        productService.getAllCount(),
                        productService.getAll(size, (page - 1) * size)
                ).asTuple())
                .map(tuple -> {
                    var dtos = tuple.getItem2().stream().map(SubscriptionProductDTO::from).collect(Collectors.toList());
                    var view = new com.semantyca.core.dto.view.View<>(dtos, tuple.getItem1(), page,
                            RuntimeUtil.countMaxPage(tuple.getItem1(), size), size);
                    var viewPage = new com.semantyca.core.dto.view.ViewPage();
                    viewPage.addPayload(com.semantyca.core.dto.cnst.PayloadType.VIEW_DATA, view);
                    return viewPage;
                })
                .subscribe().with(
                        vp -> rc.response().setStatusCode(200).end(JsonObject.mapFrom(vp).encode()),
                        rc::fail
                );
    }

    private void getProduct(RoutingContext rc) {
        String id = rc.pathParam("id");

        getContextUser(rc)
                .chain(user -> {
                    if ("new".equals(id)) {
                        return Uni.createFrom().item(new SubscriptionProductDTO());
                    }
                    return productService.findById(UUID.fromString(id)).map(SubscriptionProductDTO::from);
                })
                .subscribe().with(
                        dto -> {
                            var page = new com.semantyca.core.dto.form.FormPage();
                            page.addPayload(com.semantyca.core.dto.cnst.PayloadType.DOC_DATA, dto);
                            rc.response().setStatusCode(200).end(JsonObject.mapFrom(page).encode());
                        },
                        rc::fail
                );
    }

    private void upsertProduct(RoutingContext rc) {
        try {
            JsonObject json = rc.body().asJsonObject();
            if (json == null) {
                rc.response().setStatusCode(400).end("Request body must be a valid JSON object");
                return;
            }
            SubscriptionProductDTO dto = json.mapTo(SubscriptionProductDTO.class);
            String id = rc.pathParam("id");

            SubscriptionProduct doc = new SubscriptionProduct();
            doc.setIdentifier(dto.getIdentifier());
            doc.setLocalizedName(dto.getLocalizedName());
            doc.setLocalizedDescription(dto.getLocalizedDescription());
            doc.setStripePriceId(dto.getStripePriceId());
            doc.setStripeProductId(dto.getStripeProductId());
            doc.setActive(dto.isActive());
            doc.setDefaultValues(dto.getDefaultValues());

            getContextUser(rc, false, false)
                    .chain(user -> productService.upsert(id, doc, user))
                    .map(SubscriptionProductDTO::from)
                    .subscribe().with(
                            result -> rc.response()
                                    .setStatusCode(id == null ? 201 : 200)
                                    .end(JsonObject.mapFrom(result).encode()),
                            throwable -> handleFailure(rc, throwable)
                    );
        } catch (Exception e) {
            rc.response().setStatusCode(400).end("Invalid JSON payload");
        }
    }

    private void deleteProduct(RoutingContext rc) {
        String id = rc.pathParam("id");

        getContextUser(rc, false, false)
                .chain(user -> productService.delete(UUID.fromString(id)))
                .subscribe().with(
                        count -> rc.response().setStatusCode(count > 0 ? 204 : 404).end(),
                        throwable -> handleFailure(rc, throwable)
                );
    }

    private void getAll(RoutingContext rc) {
        int page = Integer.parseInt(rc.request().getParam("page", "1"));
        int size = Integer.parseInt(rc.request().getParam("size", "10"));

        getContextUser(rc)
                .chain(user -> Uni.combine().all().unis(
                        subscriptionRepository.getAllCount(),
                        subscriptionRepository.getAll(size, (page - 1) * size)
                ).asTuple())
                .chain(tuple -> {
                    var dtos = tuple.getItem2().stream().map(UserSubscriptionDTO::from).collect(Collectors.toList());
                    if (dtos.isEmpty()) {
                        var view = new com.semantyca.core.dto.view.View<>(dtos, tuple.getItem1(), page,
                                RuntimeUtil.countMaxPage(tuple.getItem1(), size), size);
                        var viewPage = new com.semantyca.core.dto.view.ViewPage();
                        viewPage.addPayload(com.semantyca.core.dto.cnst.PayloadType.VIEW_DATA, view);
                        return Uni.createFrom().item(viewPage);
                    }
                    var userUnis = dtos.stream()
                            .map(dto -> userService.findById(dto.getUserId())
                                    .map(opt -> {
                                        opt.ifPresent(u -> dto.setUser(new UserSubscriptionDTO.UserRef(u.getLogin(), u.getEmail())));
                                        return dto;
                                    }))
                            .collect(Collectors.toList());
                    return Uni.join().all(userUnis).andFailFast().map(resolved -> {
                        var view = new com.semantyca.core.dto.view.View<>(resolved, tuple.getItem1(), page,
                                RuntimeUtil.countMaxPage(tuple.getItem1(), size), size);
                        var viewPage = new com.semantyca.core.dto.view.ViewPage();
                        viewPage.addPayload(com.semantyca.core.dto.cnst.PayloadType.VIEW_DATA, view);
                        return viewPage;
                    });
                })
                .subscribe().with(
                        vp -> rc.response().setStatusCode(200).end(JsonObject.mapFrom(vp).encode()),
                        rc::fail
                );
    }

    private void get(RoutingContext rc) {
        String id = rc.pathParam("id");

        getContextUser(rc)
                .chain(user -> {
                    if ("new".equals(id)) {
                        return Uni.createFrom().item(new UserSubscriptionDTO());
                    }
                    return subscriptionRepository.findById(UUID.fromString(id))
                            .map(UserSubscriptionDTO::from)
                            .chain(dto -> userService.findById(dto.getUserId())
                                    .map(opt -> {
                                        opt.ifPresent(u -> dto.setUser(new UserSubscriptionDTO.UserRef(u.getLogin(), u.getEmail())));
                                        return dto;
                                    }));
                })
                .subscribe().with(
                        dto -> {
                            var page = new com.semantyca.core.dto.form.FormPage();
                            page.addPayload(com.semantyca.core.dto.cnst.PayloadType.DOC_DATA, dto);
                            rc.response().setStatusCode(200).end(JsonObject.mapFrom(page).encode());
                        },
                        rc::fail
                );
    }

    private void upsert(RoutingContext rc) {
        try {
            JsonObject json = rc.body().asJsonObject();
            if (json == null) {
                rc.response().setStatusCode(400).end("Request body must be a valid JSON object");
                return;
            }
            UserSubscriptionDTO dto = json.mapTo(UserSubscriptionDTO.class);
            String id = rc.pathParam("id");

            UserSubscription doc = new UserSubscription();
            doc.setUserId(dto.getUserId());
            doc.setStripeCustomerId(dto.getStripeCustomerId());
            doc.setStripeSubscriptionId(dto.getStripeSubscriptionId());
            doc.setSubscriptionType(dto.getSubscriptionType());
            doc.setSubscriptionStatus(dto.getSubscriptionStatus());
            doc.setTrialEnd(dto.getTrialEnd());
            doc.setCurrentPeriodStart(dto.getCurrentPeriodStart());
            doc.setCurrentPeriodEnd(dto.getCurrentPeriodEnd());
            doc.setCancelAt(dto.getCancelAt());
            doc.setCanceledAt(dto.getCanceledAt());
            doc.setActive(dto.isActive());
            doc.setStreamDurationMinutes(dto.getStreamDurationMinutes());
            doc.setOtsAllowed(dto.isOtsAllowed());
            doc.setMaxSongs(dto.getMaxSongs());
            doc.setStreamQualityKbps(dto.getStreamQualityKbps());
            doc.setDjTypeId(dto.getDjTypeId());
            doc.setSupportLevel(dto.getSupportLevel());
            doc.setCustomScriptAllowed(dto.isCustomScriptAllowed());

            getContextUser(rc, false, false)
                    .chain(user -> "new".equalsIgnoreCase(id) || id == null
                            ? subscriptionRepository.insert(doc, user)
                            : subscriptionRepository.update(UUID.fromString(id), doc, user))
                    .map(UserSubscriptionDTO::from)
                    .subscribe().with(
                            result -> rc.response()
                                    .setStatusCode(id == null ? 201 : 200)
                                    .end(JsonObject.mapFrom(result).encode()),
                            throwable -> handleFailure(rc, throwable)
                    );
        } catch (Exception e) {
            rc.response().setStatusCode(400).end("Invalid JSON payload");
        }
    }

    private void delete(RoutingContext rc) {
        String id = rc.pathParam("id");

        getContextUser(rc, false, false)
                .chain(user -> subscriptionRepository.delete(UUID.fromString(id)))
                .subscribe().with(
                        count -> rc.response().setStatusCode(count > 0 ? 204 : 404).end(),
                        throwable -> handleFailure(rc, throwable)
                );
    }
}
