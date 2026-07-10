package com.semantyca.datanest.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.semantyca.core.dto.AbstractDTO;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.Setter;

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
}
