package com.semantyca.datanest.rest.mixdeck;

import com.semantyca.core.dto.actions.cnst.ActionType;
import com.semantyca.datanest.service.UserSubscriptionService;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;

import java.util.ArrayList;
import java.util.List;

public final class MixdeckEntitlements {
    private MixdeckEntitlements() {
    }

    public static List<String> viewActions(boolean canCreate) {
        List<String> actions = new ArrayList<>();
        if (canCreate) {
            actions.add(ActionType.CREATE.getAlias());
        }
        actions.add(ActionType.DELETE.getAlias());
        return actions;
    }

    public static boolean respondLimitFailure(RoutingContext rc, Throwable throwable) {
        if (!(throwable instanceof UserSubscriptionService.EntitlementLimitException limit)) {
            return false;
        }
        JsonObject body = new JsonObject()
                .put("status", 403)
                .put("code", limit.getCode())
                .put("title", limit.getTitle())
                .put("detail", limit.getMessage())
                .put("upgradeTo", "Plus")
                .put("upgradeHint", limit.getUpgradeHint());
        if (limit.getSubscriptionType() != null) {
            body.put("subscriptionType", limit.getSubscriptionType());
        }
        if (limit.getMaxField() != null && limit.getMaxValue() != null) {
            body.put(limit.getMaxField(), limit.getMaxValue());
        }
        if (limit.getCountField() != null && limit.getCountValue() != null) {
            body.put(limit.getCountField(), limit.getCountValue());
        }
        rc.response()
                .setStatusCode(403)
                .putHeader("Content-Type", "application/json")
                .end(body.encode());
        return true;
    }
}
