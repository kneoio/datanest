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
public class MasterPromptTranslateReqDTO {
    /** English master instruction prompt to translate. */
    @NotBlank
    private String prompt;
    /** BCP-47 language tag for the translation target, e.g. {@code de}, {@code fr-FR}. */
    @NotBlank
    private String targetLanguageTag;
    /** Optional notes: glossary, brand terms, tone hints — not the script to rewrite. */
    private String draft;
    /** UI-selected model family (informational; request is executed with Anthropic). */
    @NotNull
    private LlmType llmType;
}
