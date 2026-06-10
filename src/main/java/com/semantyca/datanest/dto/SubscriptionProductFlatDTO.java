package com.semantyca.datanest.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.semantyca.core.dto.document.SubscriptionProductDTO;
import com.semantyca.core.model.cnst.LanguageCode;
import lombok.Getter;
import lombok.Setter;

import java.util.UUID;

@Getter
@Setter
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SubscriptionProductFlatDTO {
    private UUID id;
    private String identifier;
    private String name;
    private String description;
    private String stripePriceId;
    private String stripeProductId;
    private boolean active;

    public static SubscriptionProductFlatDTO from(SubscriptionProductDTO dto) {
        SubscriptionProductFlatDTO f = new SubscriptionProductFlatDTO();
        f.setId(dto.getId());
        f.setIdentifier(dto.getIdentifier());
        f.setStripePriceId(dto.getStripePriceId());
        f.setStripeProductId(dto.getStripeProductId());
        f.setActive(dto.isActive());
        if (dto.getLocalizedName() != null) {
            f.setName(dto.getLocalizedName().getOrDefault(LanguageCode.en,
                    dto.getLocalizedName().values().stream().findFirst().orElse(null)));
        }
        if (dto.getLocalizedDescription() != null) {
            f.setDescription(dto.getLocalizedDescription().getOrDefault(LanguageCode.en,
                    dto.getLocalizedDescription().values().stream().findFirst().orElse(null)));
        }
        return f;
    }
}
