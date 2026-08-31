package com.semantyca.datanest.rest;

import com.semantyca.core.controller.AbstractSecuredController;
import com.semantyca.core.service.UserService;
import com.semantyca.datanest.util.DocumentIds;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;

public abstract class DatanestSecuredController<T, V> extends AbstractSecuredController<T, V> {

    public DatanestSecuredController(UserService userService) {
        super(userService);
    }

    @Override
    protected void sendUpsertResponse(RoutingContext rc, Object doc, String id) {
        rc.response()
                .setStatusCode(DocumentIds.isNewDocumentId(id) ? 201 : 200)
                .putHeader("Content-Type", "application/json")
                .end(JsonObject.mapFrom(doc).encode());
    }
}
