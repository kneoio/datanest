package com.semantyca.datanest.dto.brand.mixdeck;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.semantyca.core.dto.AbstractDTO;
import com.semantyca.mixpla.model.cnst.LlmType;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Mixdeck AI agent DTO — uses {@code slugName}, no document UUID exposure.
 */
@Setter
@Getter
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties({"id"})
public class AiAgentMixdeckDTO extends AbstractDTO {
    private String name;
    private String slugName;
    private String description;
    private String manner;
    private LlmType llmType;
}
