package com.semantyca.datanest.rest.mixdeck;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.semantyca.core.dto.actions.cnst.ActionType;
import com.semantyca.datanest.service.UserSubscriptionService;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;

import java.util.List;

public final class MixdeckEntitlements {
    private MixdeckEntitlements() {
    }

    public static List<MixdeckAction> viewActions(UserSubscriptionService.CreateAvailability create) {
        return List.of(createAction(create), enabled(ActionType.DELETE));
    }

    private static MixdeckAction createAction(UserSubscriptionService.CreateAvailability create) {
        if (create != null && create.enabled()) {
            return enabled(ActionType.CREATE);
        }
        UserSubscriptionService.EntitlementLimitException denial = create != null ? create.denial() : null;
        MixdeckAction action = new MixdeckAction();
        action.setId(ActionType.CREATE.getAlias());
        action.setEnabled(false);
        if (denial != null) {
            action.setCode(denial.getCode());
            action.setReason(denial.getMessage());
        }
        return action;
    }

    private static MixdeckAction enabled(ActionType type) {
        MixdeckAction action = new MixdeckAction();
        action.setId(type.getAlias());
        action.setEnabled(true);
        return action;
    }

    public static boolean respondLimitFailure(RoutingContext rc, Throwable throwable) {
        if (!(throwable instanceof UserSubscriptionService.EntitlementLimitException limit)) {
            return false;
        }
        JsonObject body = new JsonObject()
                .put("status", 403)
                .put("code", limit.getCode())
                .put("title", limit.getTitle())
                .put("detail", limit.getMessage());
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

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class MixdeckAction {
        private String id;
        private boolean enabled;
        private String code;
        private String reason;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public String getCode() {
            return code;
        }

        public void setCode(String code) {
            this.code = code;
        }

        public String getReason() {
            return reason;
        }

        public void setReason(String reason) {
            this.reason = reason;
        }
    }
}
