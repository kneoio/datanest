package com.semantyca.datanest.dto.subscription;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.semantyca.core.model.SubscriptionProduct;
import com.semantyca.core.model.UserSubscription;
import com.semantyca.core.model.cnst.LanguageCode;
import lombok.Getter;
import lombok.Setter;

import java.util.UUID;

@Getter
@Setter
@JsonInclude(JsonInclude.Include.NON_NULL)
public class UserSubscriptionProductStatusDTO {
    private UUID id;
    private String identifier;
    private String name;
    private String description;
    private String stripePriceId;
    private String stripeProductId;
    private boolean active;
    private boolean subscribed;
    private String subscriptionStatus;

    public static UserSubscriptionProductStatusDTO from(SubscriptionProduct product, UserSubscription subscription) {
        UserSubscriptionProductStatusDTO dto = new UserSubscriptionProductStatusDTO();
        dto.setId(product.getId());
        dto.setIdentifier(product.getIdentifier());
        dto.setStripePriceId(product.getStripePriceId());
        dto.setStripeProductId(product.getStripeProductId());
        dto.setActive(product.isActive());
        if (product.getLocalizedName() != null) {
            dto.setName(product.getLocalizedName().getOrDefault(LanguageCode.en,
                    product.getLocalizedName().values().stream().findFirst().orElse(null)));
        }
        if (product.getLocalizedDescription() != null) {
            dto.setDescription(product.getLocalizedDescription().getOrDefault(LanguageCode.en,
                    product.getLocalizedDescription().values().stream().findFirst().orElse(null)));
        }
        if (subscription != null) {
            dto.setSubscribed(subscription.isActive());
            dto.setSubscriptionStatus(subscription.getSubscriptionStatus());
        }
        return dto;
    }
}
