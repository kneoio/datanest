package com.semantyca.datanest.dto.subscription;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.semantyca.core.dto.AbstractDTO;
import com.semantyca.mixpla.model.UserSubscription;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

import java.time.ZonedDateTime;
import java.util.UUID;

@Setter
@Getter
@SuperBuilder
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class UserSubscriptionDTO extends AbstractDTO {

    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class UserRef {
        private String login;
        private String email;
    }

    private long userId;
    private UserRef user;
    private String stripeCustomerId;
    private String stripeSubscriptionId;
    private String subscriptionType;
    private String subscriptionStatus;
    private ZonedDateTime trialEnd;
    private ZonedDateTime currentPeriodStart;
    private ZonedDateTime currentPeriodEnd;
    private ZonedDateTime cancelAt;
    private ZonedDateTime canceledAt;
    private boolean active;
    private Integer streamDurationMinutes;
    private boolean otsAllowed;
    private Integer maxSongs;
    private Integer streamQualityKbps;
    private UUID djTypeId;
    private short supportLevel;
    private boolean customScriptAllowed;

    public static UserSubscriptionDTO from(UserSubscription s) {
        return UserSubscriptionDTO.builder()
                .id(s.getId())
                .userId(s.getUserId())
                .stripeCustomerId(s.getStripeCustomerId())
                .stripeSubscriptionId(s.getStripeSubscriptionId())
                .subscriptionType(s.getSubscriptionType())
                .subscriptionStatus(s.getSubscriptionStatus())
                .trialEnd(s.getTrialEnd())
                .currentPeriodStart(s.getCurrentPeriodStart())
                .currentPeriodEnd(s.getCurrentPeriodEnd())
                .cancelAt(s.getCancelAt())
                .canceledAt(s.getCanceledAt())
                .active(s.isActive())
                .streamDurationMinutes(s.getStreamDurationMinutes())
                .otsAllowed(s.isOtsAllowed())
                .maxSongs(s.getMaxSongs())
                .streamQualityKbps(s.getStreamQualityKbps())
                .djTypeId(s.getDjTypeId())
                .supportLevel(s.getSupportLevel())
                .customScriptAllowed(s.isCustomScriptAllowed())
                .build();
    }
}
