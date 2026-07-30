package com.semantyca.datanest.dto.aiagent;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.semantyca.core.dto.AbstractDTO;
import com.semantyca.core.dto.rls.RlsActionDTO;
import com.semantyca.mixpla.model.cnst.LlmType;
import jakarta.validation.constraints.NotBlank;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Getter
@Setter
@JsonInclude(JsonInclude.Include.NON_NULL)
public class AiAgentDTO extends AbstractDTO {
    @NotBlank
    private String name;
    private String slugName;
    private String description;
    private String manner;
    private LlmType llmType;
    private List<LanguagePreferenceDTO> preferredLang;
    private UUID copilot;
    private TTSSettingDTO ttsSetting;
    private List<UUID> labels;
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    private List<RlsActionDTO> rlsActions = new ArrayList<>();
}
