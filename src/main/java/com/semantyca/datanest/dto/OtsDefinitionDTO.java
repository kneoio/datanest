package com.semantyca.datanest.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.semantyca.core.dto.AbstractDTO;
import com.semantyca.core.model.ScriptVariable;
import com.semantyca.mixpla.model.cnst.OtsRunStatus;
import com.semantyca.mixpla.model.cnst.OtsRunType;
import com.semantyca.mixpla.model.stream.OtsStatusHistoryEntry;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.Setter;

import java.util.List;
import java.util.Map;
@Setter
@Getter
@JsonInclude(JsonInclude.Include.NON_NULL)
public class OtsDefinitionDTO extends AbstractDTO {
    private String name;
    private String slugName;
    @NotNull
    private String scriptSlug;
    private Map<String, Object> userVariables;
    private String brandSlug;
    private String agentSlug;
    private OtsRunStatus status;
    private List<OtsStatusHistoryEntry> statusHistory;
    private OtsRunType type;
    private Integer estimatedDurationMin;
    private String chatContext;
    private String color;
    private List<ScriptVariable> requiredVariables;
}
