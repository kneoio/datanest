package com.semantyca.datanest.rest.mixdeck;

import com.semantyca.core.controller.AbstractSecuredController;
import com.semantyca.core.service.UserService;
import com.semantyca.datanest.dto.script.ScriptDTO;
import com.semantyca.datanest.service.ScriptService;
import com.semantyca.mixpla.model.Script;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.UUID;

@ApplicationScoped
public class MixdeckScriptController extends AbstractSecuredController<Script, ScriptDTO> {
    private final ScriptService service;

    public MixdeckScriptController() {
        super(null);
        this.service = null;
    }

    @Inject
    public MixdeckScriptController(UserService userService, ScriptService service) {
        super(userService);
        this.service = service;
    }

    public void setupRoutes(Router router) {
        router.route(HttpMethod.GET, "/datanest/public/scripts/:id").handler(this::getById);
    }

    private void getById(RoutingContext rc) {
        String id = rc.pathParam("id");
        try {
            UUID scriptId = UUID.fromString(id);
            assert service != null;
            service.getPublicDTO(scriptId)
                    .subscribe().with(
                            doc -> rc.response()
                                    .setStatusCode(200)
                                    .putHeader("Content-Type", "application/json")
                                    .end(io.vertx.core.json.Json.encode(doc)),
                            t -> handleFailure(rc, t)
                    );
        } catch (IllegalArgumentException e) {
            rc.fail(400, new IllegalArgumentException("Invalid script id format"));
        }
    }
}
