package com.semantyca.datanest.rest.mixdeck;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.semantyca.core.dto.cnst.PayloadType;
import com.semantyca.core.dto.form.FormPage;
import com.semantyca.core.dto.view.View;
import com.semantyca.core.dto.view.ViewPage;
import com.semantyca.core.model.cnst.LanguageCode;
import com.semantyca.core.repository.exception.DocumentHasNotFoundException;
import com.semantyca.core.repository.exception.UserNotFoundException;
import com.semantyca.core.service.UserService;
import com.semantyca.core.util.FileSecurityUtils;
import com.semantyca.core.util.ProblemDetailsUtil;
import com.semantyca.core.util.RuntimeUtil;
import com.semantyca.datanest.config.DatanestConfig;
import com.semantyca.datanest.dto.BrandSoundFragmentFlatDTO;
import com.semantyca.datanest.dto.SoundFragmentDTO;
import com.semantyca.datanest.dto.SoundFragmentPublicFlatDTO;
import com.semantyca.datanest.rest.DatanestSecuredController;
import com.semantyca.datanest.rest.SoundFragmentController;
import com.semantyca.datanest.service.soundfragment.BrandSoundFragmentService;
import com.semantyca.datanest.service.soundfragment.SharedSoundFragmentService;
import com.semantyca.datanest.service.soundfragment.SoundFragmentService;
import com.semantyca.datanest.service.UserSubscriptionService;
import com.semantyca.datanest.service.util.FileDownloadService;
import com.semantyca.datanest.service.util.ValidationResult;
import com.semantyca.datanest.service.util.ValidationService;
import com.semantyca.datanest.util.DocumentIds;
import com.semantyca.datanest.util.InputStreamReadStream;
import com.semantyca.mixpla.model.cnst.PlaylistItemType;
import com.semantyca.mixpla.model.cnst.SourceType;
import com.semantyca.mixpla.model.filter.SoundFragmentFilter;
import com.semantyca.mixpla.model.soundfragment.SoundFragment;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple2;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;
import org.jspecify.annotations.NonNull;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@ApplicationScoped
public class MixdeckSoundFragmentController extends DatanestSecuredController<SoundFragment, SoundFragmentDTO> {
    private static final Logger LOGGER = Logger.getLogger(MixdeckSoundFragmentController.class);
    private static final int STREAM_BUFFER_SIZE = 524288; // 512KB buffer for file streaming

    private final SoundFragmentService service;
    private final BrandSoundFragmentService brandSoundFragmentService;
    private final SharedSoundFragmentService sharedSoundFragmentService;
    private final FileDownloadService fileDownloadService;
    private final ValidationService validationService;
    private final UserSubscriptionService userSubscriptionService;
    private final DatanestConfig config;
    private final Vertx vertx;
    private final ObjectMapper mapper;

    public MixdeckSoundFragmentController() {
        super(null);
        this.service = null;
        this.brandSoundFragmentService = null;
        this.sharedSoundFragmentService = null;
        this.fileDownloadService = null;
        this.validationService = null;
        this.userSubscriptionService = null;
        this.config = null;
        this.vertx = null;
        this.mapper = null;
    }

    @Inject
    public MixdeckSoundFragmentController(UserService userService, SoundFragmentService service,
                                          BrandSoundFragmentService brandSoundFragmentService,
                                          SharedSoundFragmentService sharedSoundFragmentService,
                                          FileDownloadService fileDownloadService,
                                          ValidationService validationService,
                                          UserSubscriptionService userSubscriptionService,
                                          DatanestConfig config, Vertx vertx, ObjectMapper mapper) {
        super(userService);
        this.service = service;
        this.brandSoundFragmentService = brandSoundFragmentService;
        this.sharedSoundFragmentService = sharedSoundFragmentService;
        this.fileDownloadService = fileDownloadService;
        this.validationService = validationService;
        this.userSubscriptionService = userSubscriptionService;
        this.config = config;
        this.vertx = vertx;
        this.mapper = mapper;
    }

    public void setupRoutes(Router router) {
        router.route(HttpMethod.GET, "/datanest/public/soundfragments/shared").handler(this::get);
        router.route(HttpMethod.GET, "/datanest/public/soundfragments/unassigned-brands").handler(this::getUnassignedBrands); //it is archived from regular POV
        router.route(HttpMethod.GET, "/datanest/public/soundfragments/sound-assets").handler(this::getSoundAssets);
        router.route(HttpMethod.GET, "/datanest/public/soundfragments/available-soundfragments").handler(this::getForBrand);
        router.route(HttpMethod.GET, "/datanest/public/soundfragments/files/:slugName/:fileSlug").handler(this::getFileBySlugName);
        router.route(HttpMethod.PATCH, "/datanest/public/soundfragments/:slugName/boost/:brandId").handler(BodyHandler.create()).handler(this::updateBoostBySlugName);
        router.route(HttpMethod.POST, "/datanest/public/soundfragments/:slugName?").handler(BodyHandler.create()).handler(this::upsertBySlugName);
        router.route(HttpMethod.DELETE, "/datanest/public/soundfragments/:slugName").handler(this::deleteBySlugName);
        router.route(HttpMethod.GET, "/datanest/public/soundfragments/:slugName").handler(this::getBySlugName);
    }

    private void get(RoutingContext rc) {
        int page = Integer.parseInt(rc.request().getParam("page", "1"));
        int size = Integer.parseInt(rc.request().getParam("size", "10"));
        SoundFragmentFilter filter = new SoundFragmentFilter();
        filter.setShared(true);
        filter.setActivated(true);

        getContextUser(rc, false, true)
                .chain(user -> {
                    assert service != null;
                    return Uni.combine().all().unis(
                            service.getAllCount(user, filter),
                            service.getAllPublicFlatDTO(size, (page - 1) * size, user, filter)
                    ).asTuple().map(tuple -> {
                        ViewPage viewPage = new ViewPage();
                        View<SoundFragmentPublicFlatDTO> dtoEntries = new View<>(tuple.getItem2(),
                                tuple.getItem1(), page,
                                RuntimeUtil.countMaxPage(tuple.getItem1(), size),
                                size);
                        viewPage.addPayload(PayloadType.VIEW_DATA, dtoEntries);
                        return viewPage;
                    });
                })
                .subscribe().with(
                        viewPage -> {
                            try {
                                rc.response().setStatusCode(200)
                                        .putHeader("Content-Type", "application/json")
                                        .end(mapper.writeValueAsString(viewPage));
                            } catch (Exception e) {
                                rc.fail(e);
                            }
                        },
                        t -> handleFailure(rc, t)
                );
    }

    private void getBySlugName(RoutingContext rc) {
        String slugName = rc.pathParam("slugName");
        LanguageCode languageCode = LanguageCode.valueOf(rc.request().getParam("lang", LanguageCode.en.name()));

        getContextUser(rc, false, true)
                .chain(user -> {
                    assert service != null;
                    return service.getDTOBySlug(slugName, user, languageCode);
                })
                .subscribe().with(
                        doc -> {
                            FormPage page = new FormPage();
                            page.addPayload(PayloadType.DOC_DATA, doc);
                            try {
                                rc.response().setStatusCode(200)
                                        .putHeader("Content-Type", "application/json")
                                        .end(mapper.writeValueAsString(page));
                            } catch (Exception e) {
                                rc.fail(e);
                            }
                        },
                        t -> handleFailure(rc, t)
                );
    }

    private void getUnassignedBrands(RoutingContext rc) {
        int page = Integer.parseInt(rc.request().getParam("page", "1"));
        int size = Integer.parseInt(rc.request().getParam("size", "10"));
        SoundFragmentFilter filter = new SoundFragmentFilter();

        getContextUser(rc, false, true)
                .chain(user -> {
                    assert service != null;
                    assert userSubscriptionService != null;
                    return Uni.combine().all().unis(
                            service.getAllCountWithoutBrandAssociation(user, filter),
                            service.getAllPublicFlatDTOWithoutBrandAssociation(size, (page - 1) * size, user, filter),
                            userSubscriptionService.songCreate(user)
                    ).asTuple().map(tuple -> {
                        ViewPage viewPage = new ViewPage();
                        View<SoundFragmentPublicFlatDTO> dtoEntries = new View<>(tuple.getItem2(),
                                tuple.getItem1(), page,
                                RuntimeUtil.countMaxPage(tuple.getItem1(), size),
                                size);
                        viewPage.addPayload(PayloadType.VIEW_DATA, dtoEntries);
                        viewPage.addPayload(PayloadType.CONTEXT_ACTIONS, MixdeckEntitlements.viewActions(tuple.getItem3()));
                        return viewPage;
                    });
                })
                .subscribe().with(
                        viewPage -> {
                            try {
                                rc.response().setStatusCode(200)
                                        .putHeader("Content-Type", "application/json")
                                        .end(mapper.writeValueAsString(viewPage));
                            } catch (Exception e) {
                                rc.fail(e);
                            }
                        },
                        t -> handleFailure(rc, t)
                );
    }

    private void getSoundAssets(RoutingContext rc) {
        int page = Integer.parseInt(rc.request().getParam("page", "1"));
        int size = Integer.parseInt(rc.request().getParam("size", "10"));
        SoundFragmentFilter filter = createFilter();

        getContextUser(rc, false, true)
                .chain(user -> {
                    assert service != null;
                    return Uni.combine().all().unis(
                            service.getAllCount(user, filter),
                            service.getAllPublicFlatDTO(size, (page - 1) * size, user, filter)
                    ).asTuple().map(tuple -> {
                        ViewPage viewPage = new ViewPage();
                        View<SoundFragmentPublicFlatDTO> dtoEntries = new View<>(tuple.getItem2(),
                                tuple.getItem1(), page,
                                RuntimeUtil.countMaxPage(tuple.getItem1(), size),
                                size);
                        viewPage.addPayload(PayloadType.VIEW_DATA, dtoEntries);
                        return viewPage;
                    });
                })
                .subscribe().with(
                        viewPage -> {
                            try {
                                rc.response().setStatusCode(200)
                                        .putHeader("Content-Type", "application/json")
                                        .end(mapper.writeValueAsString(viewPage));
                            } catch (Exception e) {
                                rc.fail(e);
                            }
                        },
                        t -> handleFailure(rc, t)
                );
    }

    private static @NonNull SoundFragmentFilter createFilter() {
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
        return filter;
    }

    private void getForBrand(RoutingContext rc) {
        String brandName = rc.request().getParam("brand");
        int page = Integer.parseInt(rc.request().getParam("page", "1"));
        int size = Integer.parseInt(rc.request().getParam("size", "10"));
        SoundFragmentFilter filter = SoundFragmentController.parseFilterDTO(rc,
                List.of(SourceType.USER_UPLOAD, SourceType.CONTRIBUTION), List.of(PlaylistItemType.SONG));

        getContextUser(rc, false, true)
                .chain(user -> {
                    assert brandSoundFragmentService != null;
                    return Uni.combine().all().unis(
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
                    });
                })
                .subscribe().with(
                        viewPage -> {
                            try {
                                rc.response().setStatusCode(200)
                                        .putHeader("Content-Type", "application/json")
                                        .end(mapper.writeValueAsString(viewPage));
                            } catch (Exception e) {
                                rc.fail(e);
                            }
                        },
                        t -> handleFailure(rc, t)
                );
    }

    private void upsertBySlugName(RoutingContext rc) {
        try {
            if (!validateJsonBody(rc)) {
                return;
            }

            JsonObject body = rc.body().asJsonObject();
            body.remove("id");
            List<String> genreIdentifiers = readStringList(body, "genres");
            List<String> labelIdentifiers = readStringList(body, "labels");
            body.remove("genres");
            body.remove("labels");
            SoundFragmentDTO dto = body.mapTo(SoundFragmentDTO.class);
            String slugName = rc.pathParam("slugName");

            getContextUser(rc, false, true)
                    .chain(user -> {
                        assert service != null;
                        return service.applyGenreAndLabelIdentifiers(dto, genreIdentifiers, labelIdentifiers)
                                .chain(() -> {
                                    if (dto.getType() != null && !PlaylistItemType.SONG.equals(dto.getType())) {
                                        assert config != null;
                                        config.getOtherGenreId().ifPresent(genreId ->
                                                dto.setGenres(List.of(UUID.fromString(genreId))));
                                    }

                                    assert validationService != null;
                                    ValidationResult validationResult = validationService.validateSoundFragmentDTO(slugName, dto);
                                    if (!validationResult.valid()) {
                                        ProblemDetailsUtil.respondValidationError(rc, validationResult.errorMessage(), validationResult.fieldErrors());
                                        return Uni.createFrom().nullItem();
                                    }

                                    if (DocumentIds.isNewDocumentId(slugName)) {
                                        assert userSubscriptionService != null;
                                        return userSubscriptionService.assertCanCreateSong(user)
                                                .chain(() -> service.upsert(null, dto, user, LanguageCode.en)
                                                        .map(doc -> Tuple2.of((String) null, doc)));
                                    }
                                    return service.getIdBySlug(slugName, user)
                                            .chain(id -> service.upsert(id.toString(), dto, user, LanguageCode.en)
                                                    .map(doc -> Tuple2.of(id.toString(), doc)));
                                });
                    })
                    .subscribe().with(
                            tuple -> {
                                if (tuple != null) {
                                    sendUpsertResponse(rc, tuple.getItem2(), tuple.getItem1());
                                }
                            },
                            t -> handleFailure(rc, t)
                    );

        } catch (Exception e) {
            if (e instanceof IllegalArgumentException) {
                rc.fail(400, e);
            } else {
                rc.fail(400, new IllegalArgumentException("Invalid JSON payload"));
            }
        }
    }

    private static List<String> readStringList(JsonObject body, String field) {
        var arr = body.getJsonArray(field);
        if (arr == null) {
            return null;
        }
        List<String> out = new ArrayList<>(arr.size());
        for (int i = 0; i < arr.size(); i++) {
            out.add(arr.getString(i));
        }
        return out;
    }

    private void deleteBySlugName(RoutingContext rc) {
        String slugName = rc.pathParam("slugName");

        getContextUser(rc, false, true)
                .chain(user -> {
                    assert service != null;
                    return service.getIdBySlug(slugName, user)
                            .chain(id -> service.archive(id.toString(), user));
                })
                .subscribe().with(
                        count -> rc.response().setStatusCode(count > 0 ? 204 : 404).end(),
                        t -> handleFailure(rc, t)
                );
    }

    private void updateBoostBySlugName(RoutingContext rc) {
        String slugName = rc.pathParam("slugName");
        String brandSlug = rc.pathParam("brandId");
        try {
            var body = rc.body().asJsonObject();
            Integer boost = body.getInteger("boost");
            String type = body.getString("type");
            if (boost == null) {
                rc.fail(400, new IllegalArgumentException("'boost' field is required"));
                return;
            }
            if (type == null || (!type.equals("brand") && !type.equals("shared"))) {
                rc.fail(400, new IllegalArgumentException("'type' must be 'brand' or 'shared'"));
                return;
            }
            if (boost < -1 || boost > 2) {
                rc.fail(400, new IllegalArgumentException("boost must be between -1 and 2"));
                return;
            }

            getContextUser(rc, false, true)
                    .chain(user -> {
                        assert service != null;
                        return service.getIdBySlug(slugName, user)
                                .chain(soundFragmentId -> service.resolveBrandSlug(brandSlug)
                                        .chain(brandUUID -> {
                                            if (brandUUID == null) {
                                                return Uni.createFrom().failure(new IllegalArgumentException("Brand not found: " + brandSlug));
                                            }
                                            if ("shared".equals(type)) {
                                                assert sharedSoundFragmentService != null;
                                                return sharedSoundFragmentService.updateBoostBySoundFragmentAndBrand(soundFragmentId, brandUUID, boost);
                                            }
                                            return service.updateBoost(soundFragmentId.toString(), brandUUID.toString(), boost, type);
                                        }));
                    })
                    .subscribe().with(
                            ignored -> rc.response().setStatusCode(204).end(),
                            t -> handleFailure(rc, t)
                    );
        } catch (IllegalArgumentException e) {
            rc.fail(400, new IllegalArgumentException("Invalid ID format"));
        }
    }

    private void getFileBySlugName(RoutingContext rc) {
        String slugName = rc.pathParam("slugName");
        String requestedFileName = rc.pathParam("fileSlug");

        getContextUser(rc, false, true)
                .chain(user -> {
                    assert service != null;
                    return service.getIdBySlug(slugName, user)
                            .chain(id -> {
                                assert fileDownloadService != null;
                                return fileDownloadService.getFile(id.toString(), requestedFileName, user);
                            });
                })
                .subscribe().with(
                        fileData -> {
                            if (fileData == null ||
                                    (fileData.getData() == null && fileData.getInputStream() == null) ||
                                    (fileData.hasByteArray() && fileData.getData().length == 0)) {
                                rc.fail(404, new IllegalArgumentException("File content not available"));
                                return;
                            }

                            HttpServerResponse response = rc.response()
                                    .putHeader("Content-Disposition", "attachment; filename=\"" +
                                            FileSecurityUtils.sanitizeFilename(requestedFileName) + "\"")
                                    .putHeader("Content-Type", fileData.getMimeType())
                                    .putHeader("Content-Length", String.valueOf(fileData.getContentLength()));

                            if (fileData.hasByteArray()) {
                                response.end(Buffer.buffer(fileData.getData()));
                            } else if (fileData.hasInputStream()) {
                                response.setChunked(true);

                                InputStreamReadStream inputStreamReadStream = new InputStreamReadStream(vertx, fileData.getInputStream(), STREAM_BUFFER_SIZE);
                                inputStreamReadStream.pipeTo(response)
                                        .onComplete(ar -> {
                                            if (ar.failed()) {
                                                LOGGER.error("Stream failed", ar.cause());
                                                if (!response.ended()) {
                                                    response.setStatusCode(500).end();
                                                }
                                            }
                                        });
                            }
                        },
                        t -> handleFailure(rc, t)
                );
    }

    protected void handleFailure(RoutingContext rc, Throwable throwable) {
        if (MixdeckEntitlements.respondLimitFailure(rc, throwable)) {
            return;
        }
        if (throwable instanceof DocumentHasNotFoundException) {
            rc.fail(404, throwable);
        } else if (throwable instanceof IllegalStateException
                || throwable instanceof IllegalArgumentException
                || throwable instanceof UserNotFoundException) {
            rc.fail(401, throwable);
        } else {
            rc.fail(throwable);
        }
    }
}
