package com.semantyca.datanest.dto.brand.mixdeck;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.semantyca.datanest.dto.aiagent.LanguagePreferenceDTO;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class AiAgentMixdeckFlatDTO {
    private String slugName;
    private String name;
    private String description;
    private String manner;
    private List<LanguagePreferenceDTO> preferredLang;
    private List<LabelMixdeckFlatDTO> labels;
}
