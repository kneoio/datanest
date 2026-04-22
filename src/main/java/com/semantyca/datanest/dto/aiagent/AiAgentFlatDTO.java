package com.semantyca.datanest.dto.aiagent;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.semantyca.datanest.dto.LabelFlatDTO;
import jakarta.validation.constraints.NotBlank;
import lombok.Getter;
import lombok.Setter;

import java.util.List;
import java.util.UUID;

@Getter
@Setter
@JsonInclude(JsonInclude.Include.NON_NULL)
public class AiAgentFlatDTO {
    private UUID id;
    @NotBlank
    private String name;
    private String description;
    private List<LanguagePreferenceDTO> preferredLang;
    private List<LabelFlatDTO> labels;
}
