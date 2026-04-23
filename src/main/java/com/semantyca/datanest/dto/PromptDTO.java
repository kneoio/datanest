package com.semantyca.datanest.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.semantyca.core.dto.AbstractDTO;
import com.semantyca.datanest.dto.RlsActionDTO;
import com.semantyca.mixpla.model.cnst.PromptType;
import io.vertx.core.json.JsonObject;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Getter
@Setter
public class PromptDTO extends AbstractDTO {
    private boolean enabled;
    private String prompt;
    private String description;
    private PromptType promptType;
    private String languageTag;
    private boolean master;
    private boolean locked;
    private String title;
    private JsonObject backup;
    private boolean podcast;
    private UUID draftId;
    private UUID masterId;
    private double version;
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    private List<RlsActionDTO> rlsActions = new ArrayList<>();
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    private List<PromptDTO> children = new ArrayList<>();
}
