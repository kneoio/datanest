package com.semantyca.datanest.dto.agentrest;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.semantyca.mixpla.model.cnst.LlmType;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@JsonInclude(JsonInclude.Include.NON_NULL)
public class PromptTestReqDTO {
    @NotBlank
    private String prompt;
    @NotBlank
    private String draft;
    @NotNull
    private LlmType llmType;
}