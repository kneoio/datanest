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
import com.semantyca.core.util.FileSecurityUtils;
import com.semantyca.core.util.ProblemDetailsUtil;
import com.semantyca.core.util.RuntimeUtil;
import com.semantyca.datanest.dto.BrandSoundFragmentFlatDTO;
import com.semantyca.datanest.dto.BulkBrandUpdateDTO;
import com.semantyca.datanest.dto.SoundFragmentDTO;
import com.semantyca.datanest.dto.actions.SoundFragmentActionsFactory;
import com.semantyca.datanest.service.soundfragment.BrandSoundFragmentService;
import com.semantyca.datanest.service.soundfragment.SoundFragmentService;
import com.semantyca.datanest.service.util.FileDownloadService;
import com.semantyca.datanest.service.util.ValidationResult;
import com.semantyca.datanest.service.util.ValidationService;
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
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@ApplicationScoped
public class SoundFragmentController extends AbstractSecuredController<SoundFragment, SoundFragmentDTO> {
    private static final Logger LOGGER = Logger.getLogger(SoundFragmentController.class);
    private static final int STREAM_BUFFER_SIZE = 524288; // 512KB buffer for file streaming

    private SoundFragmentService service;
    private BrandSoundFragmentService brandSoundFragmentService;
    private FileDownloadService fileDownloadService;
    private ValidationService validationService;
    private Vertx vertx;

    public SoundFragmentController() {
        super(null);
    }

    @Inject
    public SoundFragmentController(UserService userService,
                                   SoundFragmentService service,
                                   BrandSoundFragmentService brandSoundFragmentService,
                                   FileDownloadService fileDownloadService,
                                   ValidationService validationService,
                                   Vertx vertx) {
        super(userService);
        this.service = service;
        this.brandSoundFragmentService = brandSoundFragmentService;
        this.fileDownloadService = fileDownloadService;
        this.validationService = validationService;
        this.vertx = vertx;
    }

    public void setupRoutes(Router router) {
        String path = "/datanest/soundfragments";
        BodyHandler jsonBodyHandler = BodyHandler.create().setHandleFileUploads(false);
        router.route(HttpMethod.GET, path).handler(this::get);
        router.route(HttpMethod.GET, path + "/available-soundfragments").handler(this::getForBrand);
        router.route(HttpMethod.GET, path + "/unassigned-brands").handler(this::getUnassignedBrands); //archived in regular user context
        router.route(HttpMethod.GET, path + "/:id").handler(this::getById);
        router.route(HttpMethod.GET, path + "/files/:id/:slug").handler(this::getBySlugName);
        router.route(HttpMethod.POST, path + "/bulk-brand-update").handler(jsonBodyHandler).handler(this::bulkBrandUpdate);
        router.route(HttpMethod.POST, path + "/:id?").handler(jsonBodyHandler).handler(this::upsert);
        router.route(HttpMethod.DELETE, path + "/:id").handler(this::delete);
        router.route(HttpMethod.DELETE, path + "/:id/access").handler(this::revokeMyAccess);
        router.route(HttpMethod.GET, path + "/:id/access").handler(this::getDocumentAccess);

    }

    private void get(RoutingContext rc) {
        int page = Integer.parseInt(rc.request().getParam("page", "1"));
        int size = Integer.parseInt(rc.request().getParam("size", "10"));
        SoundFragmentFilter filter = parseFilterDTOForAdmin(rc);

        getContextUser(rc, false, true)
                .chain(user -> Uni.combine().all().unis(
                        service.getAllCount(user, filter),
                        service.getAllDTO(size, (page - 1) * size, user, filter)
                ).asTuple().map(tuple -> {
                    ViewPage viewPage = new ViewPage();
                    View<SoundFragmentDTO> dtoEntries = new View<>(tuple.getItem2(),
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

    private void getUnassignedBrands(RoutingContext rc) {
        int page = Integer.parseInt(rc.request().getParam("page", "1"));
        int size = Integer.parseInt(rc.request().getParam("size", "10"));
        SoundFragmentFilter filter = parseFilterDTOForAdmin(rc);

        getContextUser(rc, false, true)
                .chain(user -> Uni.combine().all().unis(
                        service.getAllCountWithoutBrandAssociation(user, filter),
                        service.getAllDTOWithoutBrandAssociation(size, (page - 1) * size, user, filter)
                ).asTuple().map(tuple -> {
                    ViewPage viewPage = new ViewPage();
                    View<SoundFragmentDTO> dtoEntries = new View<>(tuple.getItem2(),
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

    private void getForBrand(RoutingContext rc) {
        String brandName = rc.request().getParam("brand");
        int page = Integer.parseInt(rc.request().getParam("page", "1"));
        int size = Integer.parseInt(rc.request().getParam("size", "10"));
        SoundFragmentFilter filter = parseFilterDTOForBrand(rc);

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

    private void upsert(RoutingContext rc) {
        try {
            if (!validateJsonBody(rc)) {
                return;
            }

            SoundFragmentDTO dto = rc.body().asJsonObject().mapTo(SoundFragmentDTO.class);
            String id = rc.pathParam("id");

            ValidationResult validationResult = validationService.validateSoundFragmentDTO(id, dto);
            if (!validationResult.valid()) {
                ProblemDetailsUtil.respondValidationError(rc, validationResult.errorMessage(), validationResult.fieldErrors());
                return;
            }

            getContextUser(rc, false, true)
                    .chain(user -> service.upsert(id, dto, user, LanguageCode.en))
                    .subscribe().with(
                            doc -> sendUpsertResponse(rc, doc, id),
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

    private void delete(RoutingContext rc) {
        String id = rc.pathParam("id");
        getContextUser(rc, false, true)
                .chain(user -> service.archive(id, user))
                .subscribe().with(
                        count -> rc.response().setStatusCode(count > 0 ? 204 : 404).end(),
                        t -> handleFailure(rc, t)
                );
    }

    private void bulkBrandUpdate(RoutingContext rc) {
        try {
            if (!validateJsonBody(rc)) {
                return;
            }

            BulkBrandUpdateDTO dto = rc.body().asJsonObject().mapTo(BulkBrandUpdateDTO.class);

            if (dto.getDocumentIds() == null || dto.getDocumentIds().isEmpty()) {
                rc.fail(400, new IllegalArgumentException("Document IDs are required"));
                return;
            }

            if (dto.getOperation() == null || (!dto.getOperation().equals("SET") && !dto.getOperation().equals("UNSET"))) {
                rc.fail(400, new IllegalArgumentException("Operation must be SET or UNSET"));
                return;
            }

            if ("SET".equals(dto.getOperation()) && (dto.getBrands() == null || dto.getBrands().isEmpty())) {
                rc.fail(400, new IllegalArgumentException("Brands list is required for SET operation"));
                return;
            }

            getContextUser(rc, false, true)
                    .chain(user -> service.bulkBrandUpdate(dto.getDocumentIds(), dto.getBrands(), dto.getOperation(), user))
                    .subscribe().with(
                            updatedCount -> {
                                JsonObject response = new JsonObject();
                                response.put("updatedCount", updatedCount);
                                response.put("operation", dto.getOperation());
                                response.put("brands", dto.getBrands());
                                rc.response()
                                        .setStatusCode(200)
                                        .putHeader("Content-Type", "application/json")
                                        .end(response.encode());
                            },
                            t -> handleFailure(rc, t)
                    );

        } catch (Exception e) {
            LOGGER.error("Error parsing bulk brand update request", e);
            rc.fail(400, new IllegalArgumentException("Invalid JSON payload"));
        }
    }



    private void getBySlugName(RoutingContext rc) {
        String id = rc.pathParam("id");
        String requestedFileName = rc.pathParam("slug");

        getContextUser(rc, false, true)
                .chain(user -> fileDownloadService.getFile(id, requestedFileName, user))
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

    private void revokeMyAccess(RoutingContext rc) {
        String id = rc.pathParam("id");
        try {
            UUID documentId = UUID.fromString(id);
            getContextUser(rc, false, true)
                    .chain(user -> service.revokeMyAccess(documentId, user))
                    .subscribe().with(
                            count -> rc.response().setStatusCode(count > 0 ? 204 : 404).end(),
                            t -> handleFailure(rc, t)
                    );
        } catch (IllegalArgumentException e) {
            rc.fail(400, new IllegalArgumentException("Invalid document ID format"));
        }
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
                            t -> handleFailure(rc, t)
                    );
        } catch (IllegalArgumentException e) {
            rc.fail(400, new IllegalArgumentException("Invalid document ID format"));
        }
    }


    private SoundFragmentFilter parseFilterDTOForAdmin(RoutingContext rc) {
        return parseFilterDTO(rc, null);
    }

    private SoundFragmentFilter parseFilterDTOForBrand(RoutingContext rc) {
        return parseFilterDTO(rc, List.of(SourceType.USER_UPLOAD, SourceType.CONTRIBUTION));
    }

    SoundFragmentFilter parseFilterDTO(RoutingContext rc, List<SourceType> allowedSources) {
        String filterParam = rc.request().getParam("filter");
        if (filterParam == null || filterParam.trim().isEmpty()) {
            SoundFragmentFilter dto = new SoundFragmentFilter();
            if (allowedSources != null) {
                dto.setSource(allowedSources);
            }
            return dto;
        }
        SoundFragmentFilter dto = new SoundFragmentFilter();
        if (allowedSources != null) {
            dto.setSource(allowedSources);
        }
        boolean hasConcreteFilters = false;
        try {
            JsonObject json = new JsonObject(filterParam);
            JsonArray g = json.getJsonArray("genre");
            if (g != null && !g.isEmpty()) {
                List<UUID> genres = new ArrayList<>();
                for (Object o : g) {
                    if (o instanceof String s) {
                        try {
                            genres.add(UUID.fromString(s));
                        } catch (IllegalArgumentException ignored) {
                        }
                    }
                }
                if (!genres.isEmpty()) {
                    dto.setGenre(genres);
                    hasConcreteFilters = true;
                }
            }
            JsonArray s = json.getJsonArray("source");
            if (s != null && !s.isEmpty()) {
                List<SourceType> requestedSources = new ArrayList<>();
                for (Object o : s) {
                    if (o instanceof String str) {
                        try {
                            SourceType sourceType = SourceType.valueOf(str);
                            boolean sourceAllowed = allowedSources == null || allowedSources.contains(sourceType);
                            if (sourceAllowed && !requestedSources.contains(sourceType)) {
                                requestedSources.add(sourceType);
                            }
                        } catch (IllegalArgumentException ignored) {
                        }
                    }
                }
                if (!requestedSources.isEmpty()) {
                    dto.setSource(requestedSources);
                    hasConcreteFilters = true;
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
                    hasConcreteFilters = true;
                }
            }
            JsonArray t = json.getJsonArray("type");
            if (t != null && !t.isEmpty()) {
                List<PlaylistItemType> requestedTypes = new ArrayList<>();
                for (Object o : t) {
                    if (o instanceof String str) {
                        try {
                            PlaylistItemType playlistItemType = PlaylistItemType.valueOf(str);
                            if (!requestedTypes.contains(playlistItemType)) {
                                requestedTypes.add(playlistItemType);
                            }
                        } catch (IllegalArgumentException ignored) {
                        }
                    }
                }
                if (!requestedTypes.isEmpty()) {
                    dto.setType(requestedTypes);
                    hasConcreteFilters = true;
                }
            }
            String searchTerm = json.getString("searchTerm");
            if (searchTerm != null && !searchTerm.trim().isEmpty()) {
                dto.setSearchTerm(searchTerm.trim());
                hasConcreteFilters = true;
            }
            Integer author = json.getInteger("author");
            if (author != null && author > 0) {
                dto.setAuthor(author);
                hasConcreteFilters = true;
            }
            JsonArray b = json.getJsonArray("brands");
            if (b != null && !b.isEmpty()) {
                List<UUID> brands = new ArrayList<>();
                for (Object o : b) {
                    if (o instanceof String str) {
                        try {
                            brands.add(UUID.fromString(str));
                        } catch (IllegalArgumentException ignored) {
                        }
                    }
                }
                if (!brands.isEmpty()) {
                    dto.setBrands(brands);
                    hasConcreteFilters = true;
                }
            }
            Boolean shared = json.getBoolean("shared");
            if (shared != null && shared) {
                dto.setShared(true);
                hasConcreteFilters = true;
            }
            if (json.containsKey("activated")) {
                dto.setActivated(json.getBoolean("activated", false));
            } else if (json.containsKey("filterActivated")) {
                dto.setActivated(json.getBoolean("filterActivated", false));
            } else if (hasConcreteFilters) {
                dto.setActivated(true);
            }
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid filter JSON format: " + e.getMessage(), e);
        }
        return dto;
    }

    protected void handleFailure(RoutingContext rc, Throwable throwable) {
        if (throwable instanceof IllegalStateException
                || throwable instanceof IllegalArgumentException
                || throwable instanceof UserNotFoundException) {
            rc.fail(401, throwable);
        } else {
            rc.fail(throwable); // default bubbling
        }
    }
}
