package com.semantyca.datanest.rest.mixdeck;

import com.semantyca.core.controller.AbstractSecuredController;
import com.semantyca.core.dto.actions.cnst.ActionType;
import com.semantyca.core.dto.cnst.PayloadType;
import com.semantyca.core.dto.form.FormPage;
import com.semantyca.core.dto.view.View;
import com.semantyca.core.dto.view.ViewPage;
import com.semantyca.core.model.cnst.LanguageCode;
import com.semantyca.core.service.UserService;
import com.semantyca.core.util.ProblemDetailsUtil;
import com.semantyca.core.util.RuntimeUtil;
import com.semantyca.core.util.WebHelper;
import com.semantyca.datanest.dto.brand.mixdeck.BrandMixdeckDTO;
import com.semantyca.datanest.dto.brand.mixdeck.BrandPublicFlatDTO;
import com.semantyca.datanest.rest.BrandController;
import com.semantyca.datanest.util.DocumentIds;
import com.semantyca.datanest.service.BrandPubService;
import com.semantyca.datanest.service.BrandService;
import com.semantyca.datanest.service.UserSubscriptionService;
import com.semantyca.mixpla.model.brand.Brand;
import com.semantyca.mixpla.model.filter.BrandFilter;
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
import org.jboss.logging.Logger;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@ApplicationScoped
public class MixdeckBrandController extends AbstractSecuredController<Brand, BrandMixdeckDTO> {
    private static final Logger LOGGER = Logger.getLogger(MixdeckBrandController.class);

    private final BrandService service;
    private final BrandPubService pubService;
    private final UserSubscriptionService userSubscriptionService;
    private final Validator validator;

    public MixdeckBrandController() {
        super(null);
        this.service = null;
        this.pubService = null;
        this.userSubscriptionService = null;
        this.validator = null;
    }

    @Inject
    public MixdeckBrandController(UserService userService, BrandService service, BrandPubService pubService,
                                  UserSubscriptionService userSubscriptionService, Validator validator) {
        super(userService);
        this.service = service;
        this.pubService = pubService;
        this.userSubscriptionService = userSubscriptionService;
        this.validator = validator;
    }

    public void setupRoutes(Router router) {
        router.route(HttpMethod.GET, "/datanest/public/brands").handler(this::getAll);
        router.route(HttpMethod.POST, "/datanest/public/brands/:slugName/close").handler(BodyHandler.create()).handler(this::closeBrand);
        router.route(HttpMethod.POST, "/datanest/public/brands/:slugName?").handler(BodyHandler.create()).handler(this::upsertBySlugName);
        router.route(HttpMethod.GET, "/datanest/public/brands/:slugName").handler(this::getBySlugName);
    }

    private void getAll(RoutingContext rc) {
        int page = Integer.parseInt(rc.request().getParam("page", "1"));
        int size = Integer.parseInt(rc.request().getParam("size", "10"));
        BrandFilter filter = BrandController.parseFilterDTO(rc);

        getContextUser(rc, false, true)
                .chain(user -> {
                    assert service != null;
                    assert userSubscriptionService != null;
                    return Uni.combine().all().unis(
                                    service.getAllPublicFlatDTO(user, filter),
                                    userSubscriptionService.getActiveSubscription(user))
                            .asTuple();
                })
                .map(tuple -> {
                    var list = tuple.getItem1();
                    ViewPage viewPage = new ViewPage();
                    View<BrandPublicFlatDTO> dtoEntries = new View<>(list,
                            list.size(), page,
                            RuntimeUtil.countMaxPage(list.size(), size),
                            size);
                    viewPage.addPayload(PayloadType.VIEW_DATA, dtoEntries);
                    viewPage.addPayload(PayloadType.CONTEXT_ACTIONS, viewActions(
                            UserSubscriptionService.canCreate(tuple.getItem2(), list.size())));
                    return viewPage;
                })
                .subscribe().with(
                        viewPage -> rc.response()
                                .setStatusCode(200)
                                .putHeader("Content-Type", "application/json")
                                .end(io.vertx.core.json.Json.encode(viewPage)),
                        throwable -> {
                            LOGGER.error("Failed to get all brands", throwable);
                            rc.fail(throwable);
                        }
                );
    }

    private void getBySlugName(RoutingContext rc) {
        String slugName = rc.pathParam("slugName");
        LanguageCode languageCode = LanguageCode.valueOf(rc.request().getParam("lang", LanguageCode.en.name()));

        getContextUser(rc, false, true)
                .chain(user -> {
                    if ("new".equalsIgnoreCase(slugName)) {
                        BrandMixdeckDTO dto = new BrandMixdeckDTO();
                        dto.setLocalizedName(new EnumMap<>(LanguageCode.class));
                        dto.getLocalizedName().put(LanguageCode.en, "");
                        dto.setColor(WebHelper.generateRandomBrightColor());
                        dto.setBitRate(64000);
                        return Uni.createFrom().item(dto);
                    }
                    assert service != null;
                    return service.getDTOBySlug(slugName, user, languageCode);
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
                        throwable -> {
                            LOGGER.error("Failed to get brand by slug: " + slugName, throwable);
                            rc.fail(throwable);
                        }
                );
    }

    /** Mixdeck public upsert; path key is brand slug. Script entries use slugName. */
    private void upsertBySlugName(RoutingContext rc) {
        try {
            if (!validateJsonBody(rc)) {
                return;
            }

            String slugName = rc.pathParam("slugName");
            var body = rc.body().asJsonObject();
            BrandMixdeckDTO dto = body.mapTo(BrandMixdeckDTO.class);

            Set<ConstraintViolation<BrandMixdeckDTO>> violations = validator.validate(dto);
            if (violations != null && !violations.isEmpty()) {
                Map<String, List<String>> fieldErrors = new HashMap<>();
                for (ConstraintViolation<BrandMixdeckDTO> v : violations) {
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
            String upsertKey = isNew ? "new" : slugName;

            getContextUser(rc, false, true)
                    .chain(user -> {
                        assert pubService != null;
                        assert userSubscriptionService != null;
                        Uni<Void> guard = isNew
                                ? userSubscriptionService.assertCanCreateStation(user)
                                : Uni.createFrom().voidItem();
                        return guard.chain(() -> pubService.upsert(upsertKey, dto, user, LanguageCode.en));
                    })
                    .subscribe().with(
                            doc -> rc.response()
                                    .setStatusCode(isNew ? 201 : 200)
                                    .putHeader("Content-Type", "application/json")
                                    .end(io.vertx.core.json.Json.encode(doc)),
                            throwable -> failStationLimitOrDefault(rc, throwable, "Failed to upsert brand by slug: " + slugName)
                    );
        } catch (Exception e) {
            rc.fail(400, e instanceof IllegalArgumentException ? e : new IllegalArgumentException("Invalid JSON payload"));
        }
    }

    private static List<String> viewActions(boolean canCreate) {
        List<String> actions = new ArrayList<>();
        if (canCreate) {
            actions.add(ActionType.CREATE.getAlias());
        }
        actions.add(ActionType.DELETE.getAlias());
        return actions;
    }

    private void failStationLimitOrDefault(RoutingContext rc, Throwable throwable, String logMessage) {
        if (throwable instanceof UserSubscriptionService.StationLimitException limit) {
            JsonObject body = new JsonObject()
                    .put("status", 403)
                    .put("code", "STATION_LIMIT_REACHED")
                    .put("title", "Station limit reached")
                    .put("detail", limit.getMessage())
                    .put("upgradeTo", "Plus")
                    .put("upgradeHint", "With a Plus subscription you can create a brand.");
            if (limit.getSubscriptionType() != null) {
                body.put("subscriptionType", limit.getSubscriptionType());
            }
            if (limit.getMaxStations() != null) {
                body.put("maxStations", limit.getMaxStations());
            }
            if (limit.getStationCount() != null) {
                body.put("stationCount", limit.getStationCount());
            }
            rc.response()
                    .setStatusCode(403)
                    .putHeader("Content-Type", "application/json")
                    .end(body.encode());
            return;
        }
        LOGGER.error(logMessage, throwable);
        rc.fail(throwable);
    }

    private void closeBrand(RoutingContext rc) {
        String slugName = rc.pathParam("slugName");
        getContextUser(rc, false, true)
                .chain(user -> {
                    assert service != null;
                    return service.closeBrand(slugName, user);
                })
                .subscribe().with(
                        count -> rc.response().setStatusCode(count > 0 ? 204 : 404).end(),
                        throwable -> {
                            LOGGER.error("Failed to close brand by slug: " + slugName, throwable);
                            rc.fail(throwable);
                        }
                );
    }
}
