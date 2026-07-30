package com.semantyca.datanest.dto.brand.mixdeck;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.semantyca.core.dto.AbstractDTO;
import com.semantyca.core.model.ScriptVariable;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;

/**
 * Mixdeck script DTO — slugName only; no document UUID exposure.
 */
@Setter
@Getter
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties({"id"})
public class ScriptMixdeckDTO extends AbstractDTO {
    private String name;
    private String slugName;
    private String defaultProfileSlug;
    private String description;
    private boolean custom;
    private String color;
    private String timingMode;
    private List<String> labels;
    private List<ScriptVariable> requiredVariables;
}
