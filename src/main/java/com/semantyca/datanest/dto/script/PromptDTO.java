package com.semantyca.datanest.dto.script;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.semantyca.core.dto.AbstractDTO;
import com.semantyca.core.dto.rls.RlsActionDTO;
import com.semantyca.core.model.cnst.LanguageCode;
import com.semantyca.mixpla.model.cnst.PromptType;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.UUID;

@Getter
@Setter
public class PromptDTO extends AbstractDTO {
    private boolean enabled;
    @NotBlank
    private String prompt;
    private String description;
    @NotNull
    private PromptType promptType;
    @NotNull
    private String languageTag;
    private boolean master;
    private boolean locked;
    @NotBlank
    private String title;
    private JsonObject backup;
    private UUID draftId;
    private UUID masterId;
    private double version;
    private int allowAsOption;
    private EnumMap<LanguageCode, String> localizedOptionName = new EnumMap<>(LanguageCode.class);
    private JsonArray exposedVariables;
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    private List<RlsActionDTO> rlsActions = new ArrayList<>();
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    private List<PromptDTO> children = new ArrayList<>();
    private List<UUID> labels;
}
