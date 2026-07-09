package com.semantyca.datanest.dto.script;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.semantyca.core.dto.AbstractDTO;
import com.semantyca.core.dto.rls.RlsActionDTO;
import com.semantyca.core.model.ScriptVariable;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.Setter;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Setter
@Getter
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ScriptDTO extends AbstractDTO {
    @NotBlank
    private String name;
    private String slugName;
    private UUID defaultProfileId;
    @NotBlank
    private String description;
    private boolean custom;
    private String color;
    @NotNull
    private String timingMode;
    private List<UUID> labels;
    //private List<UUID> brands;
    private List<AbstractSceneDTO> scenes;
    private List<ScriptVariable> requiredVariables;
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    private List<RlsActionDTO> rlsActions = new ArrayList<>();
}
