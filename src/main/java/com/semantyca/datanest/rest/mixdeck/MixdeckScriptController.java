package com.semantyca.datanest.rest.mixdeck;

import com.semantyca.core.controller.AbstractSecuredController;
import com.semantyca.core.service.UserService;
import com.semantyca.datanest.dto.brand.mixdeck.ScriptMixdeckDTO;
import com.semantyca.datanest.service.ScriptService;
import com.semantyca.mixpla.model.Script;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class MixdeckScriptController extends AbstractSecuredController<Script, ScriptMixdeckDTO> {
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
        router.route(HttpMethod.GET, "/datanest/public/scripts/:slugName").handler(this::getBySlugName);
    }

    private void getBySlugName(RoutingContext rc) {
        String slugName = rc.pathParam("slugName");
        assert service != null;
        service.getMixdeckDTOBySlug(slugName)
                .subscribe().with(
                        doc -> rc.response()
                                .setStatusCode(200)
                                .putHeader("Content-Type", "application/json")
                                .end(io.vertx.core.json.Json.encode(doc)),
                        t -> handleFailure(rc, t)
                );
    }
}
