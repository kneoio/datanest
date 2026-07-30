package com.semantyca.datanest.dto.brand.mixdeck;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ScriptMixdeckFlatDTO {
    private String slugName;
    private String name;
    private String description;
    private String timingMode;
    private boolean custom;
    private List<LabelMixdeckFlatDTO> tags;
}
