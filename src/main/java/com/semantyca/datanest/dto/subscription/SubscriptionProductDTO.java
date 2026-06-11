package com.semantyca.datanest.dto.subscription;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.semantyca.core.model.cnst.LanguageCode;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

import java.util.EnumMap;
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
}
