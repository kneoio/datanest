package com.semantyca.datanest.rest;

import com.semantyca.core.controller.AbstractSecuredController;
import com.semantyca.core.dto.actions.ActionBox;
import com.semantyca.core.dto.cnst.PayloadType;
import com.semantyca.core.dto.form.FormPage;
import com.semantyca.core.dto.view.View;
import com.semantyca.core.dto.view.ViewPage;
import com.semantyca.core.model.cnst.LanguageCode;
import com.semantyca.core.repository.exception.UserNotFoundException;
import com.semantyca.core.service.UserService;
import com.semantyca.core.util.RuntimeUtil;
import com.semantyca.datanest.dto.BrandSoundFragmentFlatDTO;
import com.semantyca.datanest.dto.SoundFragmentDTO;
import com.semantyca.datanest.dto.SoundFragmentFlatDTO;
import com.semantyca.datanest.dto.actionbars.SoundFragmentActionsFactory;
import com.semantyca.datanest.service.soundfragment.BrandSoundFragmentService;
import com.semantyca.datanest.service.soundfragment.SoundFragmentService;
import com.semantyca.mixpla.model.cnst.PlaylistItemType;
import com.semantyca.mixpla.model.cnst.SourceType;
import com.semantyca.mixpla.model.filter.SoundFragmentFilter;
import com.semantyca.mixpla.model.soundfragment.SoundFragment;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple2;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.List;
import java.util.UUID;

@ApplicationScoped
public class PublicSoundFragmentController extends AbstractSecuredController<SoundFragment, SoundFragmentDTO> {
    private final SoundFragmentService service;
    private final BrandSoundFragmentService brandSoundFragmentService;

    public PublicSoundFragmentController() {
        super(null);
        this.service = null;
        this.brandSoundFragmentService = null;
    }

    @Inject
    public PublicSoundFragmentController(UserService userService, SoundFragmentService service, BrandSoundFragmentService brandSoundFragmentService) {
        super(userService);
        this.service = service;
        this.brandSoundFragmentService = brandSoundFragmentService;
    }

    public void setupRoutes(Router router) {
        router.route(HttpMethod.GET, "/datanest/public/soundfragments/shared").handler(this::get);
        router.route(HttpMethod.GET, "/datanest/public/soundfragments/unassigned-brands").handler(this::getUnassignedBrands); //it is archived from regular POV
        router.route(HttpMethod.GET, "/datanest/public/soundfragments/sound-assets").handler(this::getSoundAssets);
        router.route(HttpMethod.GET, "/datanest/public/soundfragments/available-soundfragments").handler(this::getForBrand);
        router.route(HttpMethod.GET, "/datanest/public/soundfragments/:id").handler(this::getById);
    }

    private void get(RoutingContext rc) {
        int page = Integer.parseInt(rc.request().getParam("page", "1"));
        int size = Integer.parseInt(rc.request().getParam("size", "10"));
        SoundFragmentFilter filter = new SoundFragmentFilter();
        filter.setShared(true);
        filter.setActivated(true);

        getContextUser(rc, false, true)
                .chain(user -> Uni.combine().all().unis(
                        service.getAllCount(user, filter),
                        service.getAllFlatDTO(size, (page - 1) * size, user, filter)
                ).asTuple().map(tuple -> {
                    ViewPage viewPage = new ViewPage();
                    View<SoundFragmentFlatDTO> dtoEntries = new View<>(tuple.getItem2(),
                            tuple.getItem1(), page,
                            RuntimeUtil.countMaxPage(tuple.getItem1(), size),
                            size);
                    viewPage.addPayload(PayloadType.VIEW_DATA, dtoEntries);
                    return viewPage;
                }))
                .subscribe().with(
                        viewPage -> rc.response()
                                .setStatusCode(200)
                                .putHeader("Content-Type", "application/json")
                                .end(io.vertx.core.json.Json.encode(viewPage)),
                        t -> handleFailure(rc, t)
                );
    }

    private void getById(RoutingContext rc) {
        String id = rc.pathParam("id");
        LanguageCode languageCode = LanguageCode.valueOf(rc.request().getParam("lang", LanguageCode.en.name()));

        getContextUser(rc, false, true)
                .chain(user -> {
                    if ("new".equals(id)) {
                        return service.getDTOTemplate(user, languageCode)
                                .map(dto -> Tuple2.of(dto, user));
                    }
                    return service.getDTO(UUID.fromString(id), user, languageCode)
                            .map(doc -> Tuple2.of(doc, user));
                })
                .subscribe().with(
                        tuple -> {
                            SoundFragmentDTO doc = tuple.getItem1();
                            FormPage page = new FormPage();
                            page.addPayload(PayloadType.DOC_DATA, doc);
                            rc.response()
                                    .setStatusCode(200)
                                    .putHeader("Content-Type", "application/json")
                                    .end(io.vertx.core.json.Json.encode(page));
                        },
                        t -> handleFailure(rc, t)
                );
    }

    private void getUnassignedBrands(RoutingContext rc) {
        int page = Integer.parseInt(rc.request().getParam("page", "1"));
        int size = Integer.parseInt(rc.request().getParam("size", "10"));
        SoundFragmentFilter filter = new SoundFragmentFilter();

        getContextUser(rc, false, true)
                .chain(user -> Uni.combine().all().unis(
                        service.getAllCountWithoutBrandAssociation(user, filter),
                        service.getAllFlatDTOWithoutBrandAssociation(size, (page - 1) * size, user, filter)
                ).asTuple().map(tuple -> {
                    ViewPage viewPage = new ViewPage();
                    View<SoundFragmentFlatDTO> dtoEntries = new View<>(tuple.getItem2(),
                            tuple.getItem1(), page,
                            RuntimeUtil.countMaxPage(tuple.getItem1(), size),
                            size);
                    viewPage.addPayload(PayloadType.VIEW_DATA, dtoEntries);
                    ActionBox actions = SoundFragmentActionsFactory.getViewActions();
                    viewPage.addPayload(PayloadType.CONTEXT_ACTIONS, actions);
                    return viewPage;
                }))
                .subscribe().with(
                        viewPage -> rc.response()
                                .setStatusCode(200)
                                .putHeader("Content-Type", "application/json")
                                .end(io.vertx.core.json.Json.encode(viewPage)),
                        t -> handleFailure(rc, t)
                );
    }

    private void getSoundAssets(RoutingContext rc) {
        int page = Integer.parseInt(rc.request().getParam("page", "1"));
        int size = Integer.parseInt(rc.request().getParam("size", "10"));
        SoundFragmentFilter filter = new SoundFragmentFilter();
        filter.setType(List.of(
                PlaylistItemType.ADVERTISEMENT,
                PlaylistItemType.PRERECORDED_ADVERTISEMENT,
                PlaylistItemType.PRERECORDED_PODCAST,
                PlaylistItemType.JINGLE,
                PlaylistItemType.JINGLE_INTRO,
                PlaylistItemType.JINGLE_OUTRO,
                PlaylistItemType.BACKGROUND_LOOP,
                PlaylistItemType.NEWS,
                PlaylistItemType.WEATHER
        ));
        filter.setActivated(true);

        getContextUser(rc, false, true)
                .chain(user -> Uni.combine().all().unis(
                        service.getAllCount(user, filter),
                        service.getAllFlatDTO(size, (page - 1) * size, user, filter)
                ).asTuple().map(tuple -> {
                    ViewPage viewPage = new ViewPage();
                    View<SoundFragmentFlatDTO> dtoEntries = new View<>(tuple.getItem2(),
                            tuple.getItem1(), page,
                            RuntimeUtil.countMaxPage(tuple.getItem1(), size),
                            size);
                    viewPage.addPayload(PayloadType.VIEW_DATA, dtoEntries);
                    return viewPage;
                }))
                .subscribe().with(
                        viewPage -> rc.response()
                                .setStatusCode(200)
                                .putHeader("Content-Type", "application/json")
                                .end(io.vertx.core.json.Json.encode(viewPage)),
                        t -> handleFailure(rc, t)
                );
    }

    private void getForBrand(RoutingContext rc) {
        String brandName = rc.request().getParam("brand");
        int page = Integer.parseInt(rc.request().getParam("page", "1"));
        int size = Integer.parseInt(rc.request().getParam("size", "10"));
        SoundFragmentFilter filter = SoundFragmentController.parseFilterDTO(rc,
                List.of(SourceType.USER_UPLOAD, SourceType.CONTRIBUTION), List.of(PlaylistItemType.SONG));

        getContextUser(rc, false, true)
                .chain(user -> Uni.combine().all().unis(
                        brandSoundFragmentService.getBrandSoundFragmentsFlat(brandName, size, (page - 1) * size, filter, user),
                        brandSoundFragmentService.getBrandSoundFragmentsCount(brandName, filter, user)
                ).asTuple().map(tuple -> {
                    ViewPage viewPage = new ViewPage();
                    View<BrandSoundFragmentFlatDTO> dtoEntries = new View<>(tuple.getItem1(),
                            tuple.getItem2(), page,
                            RuntimeUtil.countMaxPage(tuple.getItem2(), size),
                            size);
                    viewPage.addPayload(PayloadType.VIEW_DATA, dtoEntries);
                    return viewPage;
                }))
                .subscribe().with(
                        viewPage -> rc.response()
                                .setStatusCode(200)
                                .putHeader("Content-Type", "application/json")
                                .end(io.vertx.core.json.Json.encode(viewPage)),
                        t -> handleFailure(rc, t)
                );
    }

    protected void handleFailure(RoutingContext rc, Throwable throwable) {
        if (throwable instanceof IllegalStateException
                || throwable instanceof IllegalArgumentException
                || throwable instanceof UserNotFoundException) {
            rc.fail(401, throwable);
        } else {
            rc.fail(throwable);
        }
    }
}
