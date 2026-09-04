package com.semantyca.datanest.dto.brand.mixdeck;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Mixdeck brand-listener list row — nested listener keyed by slugName; no document UUID.
 */
@Setter
@Getter
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class BrandListenerMixdeckDTO {
    @JsonProperty("listener")
    private ListenerMixdeckDTO listenerDTO;
}
