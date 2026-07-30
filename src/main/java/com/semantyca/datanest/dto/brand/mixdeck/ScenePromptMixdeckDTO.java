package com.semantyca.datanest.dto.brand.mixdeck;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.math.BigDecimal;

@Setter
@Getter
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ScenePromptMixdeckDTO {
    private String promptSlug;
    private boolean active = true;
    private boolean mandatory = false;
    private int rank = 0;
    private BigDecimal weight = BigDecimal.valueOf(0.5);
}
