package com.semantyca.datanest.dto.subscription;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.semantyca.core.dto.AbstractDTO;
import com.semantyca.mixpla.model.MixplaUserSubscription;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.ZonedDateTime;
import java.util.List;

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
    @JsonDeserialize(using = FlexibleIntegerDeserializer.class)
    private Integer streamDurationMinutes;
    private boolean otsAllowed;
    private Integer maxSongs;
    private Integer streamQualityKbps;
    private String djTypeId;
    private short supportLevel;
    private boolean customScriptAllowed;
    private Integer maxStations;
    private boolean bulkUploadAllowed;
    @JsonAlias("price")
    private BigDecimal priceEur;
    private List<String> codecs;

    static class FlexibleIntegerDeserializer extends StdDeserializer<Integer> {
        FlexibleIntegerDeserializer() { super(Integer.class); }

        @Override
        public Integer deserialize(JsonParser p, DeserializationContext ctx) throws IOException {
            String text = p.getText().trim();
            try {
                return Integer.parseInt(text);
            } catch (NumberFormatException e) {
                return 0;
            }
        }
    }

    public static UserSubscriptionDTO from(MixplaUserSubscription s) {
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
                .djTypeId(s.getDjType() != null ? s.getDjType().toString() : null)
                .supportLevel(s.getSupportLevel())
                .customScriptAllowed(s.isCustomScriptAllowed())
                .maxStations(s.getMaxStations())
                .bulkUploadAllowed(s.isBulkUploadAllowed())
                .priceEur(s.getPriceEur())
                .build();
    }
}
