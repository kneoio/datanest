package com.semantyca.datanest.dto.subscription;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.semantyca.core.model.SubscriptionProduct;
import com.semantyca.core.model.cnst.LanguageCode;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Getter
@Setter
@SuperBuilder
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SubscriptionProductDTO {
    private UUID id;
    private String identifier;
    private String stripePriceId;
    private String stripeProductId;
    private Short orderNumber;
    @Builder.Default
    private EnumMap<LanguageCode, String> localizedName = new EnumMap<>(LanguageCode.class);
    @Builder.Default
    private EnumMap<LanguageCode, String> localizedDescription = new EnumMap<>(LanguageCode.class);
    private boolean active;
    @Builder.Default
    private Map<String, Object> defaultValues = new HashMap<>();

    public static SubscriptionProductDTO from(SubscriptionProduct doc) {
        SubscriptionProductDTO dto = new SubscriptionProductDTO();
        dto.setId(doc.getId());
        dto.setIdentifier(doc.getIdentifier());
        dto.setStripePriceId(doc.getStripePriceId());
        dto.setStripeProductId(doc.getStripeProductId());
        dto.setLocalizedName(doc.getLocalizedName());
        dto.setLocalizedDescription(doc.getLocalizedDescription());
        dto.setActive(doc.isActive());
        dto.setDefaultValues(doc.getDefaultValues() != null ? doc.getDefaultValues() : new HashMap<>());
        return dto;
    }
}
