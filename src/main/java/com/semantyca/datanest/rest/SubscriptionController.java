package com.semantyca.datanest.rest;

import com.semantyca.core.controller.AbstractSecuredController;
import com.semantyca.core.model.SubscriptionProduct;
import com.semantyca.core.model.cnst.LanguageCode;
import com.semantyca.core.dto.document.SubscriptionProductDTO;
import com.semantyca.core.service.SubscriptionProductService;
import com.semantyca.core.service.UserService;
import com.semantyca.datanest.dto.SubscriptionProductStatusDTO;
import com.semantyca.core.model.UserSubscription;
import com.semantyca.datanest.repository.UserSubscriptionRepository;
import io.smallrye.mutiny.Uni;
import io.vertx.core.json.Json;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@ApplicationScoped
public class SubscriptionController extends AbstractSecuredController<SubscriptionProduct, SubscriptionProductDTO> {

    private SubscriptionProductService productService;
    private UserSubscriptionRepository subscriptionRepository;

    public SubscriptionController() {
        super(null);
    }

    @Inject
    public SubscriptionController(UserService userService,
                                   SubscriptionProductService productService,
                                   UserSubscriptionRepository subscriptionRepository) {
        super(userService);
        this.productService = productService;
        this.subscriptionRepository = subscriptionRepository;
    }

    public void setupRoutes(Router router) {
        router.get("/datanest/subscriptions/products").handler(this::getProducts);
    }

    private void getProducts(RoutingContext rc) {
        int page = Integer.parseInt(rc.request().getParam("page", "1"));
        int size = Integer.parseInt(rc.request().getParam("size", "100"));

        getContextUser(rc, false, true)
                .chain(user -> Uni.combine().all().unis(
                        productService.getAll(size, (page - 1) * size, LanguageCode.en),
                        subscriptionRepository.findByUserId(user.getId())
                ).asTuple())
                .map(tuple -> {
                    List<SubscriptionProductDTO> products = tuple.getItem1();
                    Map<String, UserSubscription> subsByType = tuple.getItem2().stream()
                            .collect(Collectors.toMap(
                                    UserSubscription::getSubscriptionType,
                                    s -> s,
                                    (a, b) -> a.isActive() ? a : b
                            ));
                    return products.stream()
                            .map(p -> SubscriptionProductStatusDTO.from(p, subsByType.get(p.getIdentifier())))
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
}
