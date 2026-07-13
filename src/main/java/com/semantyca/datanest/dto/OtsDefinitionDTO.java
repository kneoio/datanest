package com.semantyca.datanest.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.semantyca.core.dto.AbstractDTO;
import com.semantyca.mixpla.model.cnst.OtsRunStatus;
import com.semantyca.mixpla.model.cnst.OtsRunType;
import com.semantyca.mixpla.model.stream.OtsStatusHistoryEntry;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.Setter;

import java.util.List;
import java.util.Map;
import java.util.UUID;

@Setter
@Getter
@JsonInclude(JsonInclude.Include.NON_NULL)
public class OtsDefinitionDTO extends AbstractDTO {
    private String name;
    private String slugName;
    @NotNull
    private UUID scriptId;
    private Map<String, Object> userVariables;
    private UUID brandId;
    private UUID agentId;
    private OtsRunStatus status;
    private List<OtsStatusHistoryEntry> statusHistory;
    private OtsRunType type;
    private Integer estimatedDurationMin;
    private String chatContext;
}
