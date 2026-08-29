package com.semantyca.datanest.dto.brand.mixdeck;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.semantyca.core.dto.AbstractDTO;
import com.semantyca.core.model.ScriptVariable;
import com.semantyca.mixpla.model.cnst.OtsRunStatus;
import com.semantyca.mixpla.model.cnst.OtsRunType;
import com.semantyca.mixpla.model.stream.OtsStatusHistoryEntry;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Mixdeck OTS definition DTO — slugName only; no document UUID exposure.
 */
@Setter
@Getter
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties({"id"})
public class OtsDefinitionMixdeckDTO extends AbstractDTO {
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
    /** Per-scene duration overrides (seconds). Absent/null => use each scene's own duration. */
    private Map<UUID, Integer> sceneDurations;
    /** Per-scene talkativity overrides, keyed by scene id. Absent/null => use each scene's own talkativity. */
    private Map<String, Double> sceneTalkativities;
    private int publicOts;
}
