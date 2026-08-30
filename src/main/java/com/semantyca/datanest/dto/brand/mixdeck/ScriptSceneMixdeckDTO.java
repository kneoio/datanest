package com.semantyca.datanest.dto.brand.mixdeck;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Lightweight scene summary for Mixdeck (OTS duration overrides, etc.).
 */
@Setter
@Getter
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ScriptSceneMixdeckDTO {
    private String title;
    private Integer durationSeconds;
    private double talkativity;
    private Integer seqNum;
    private boolean oneTimeRun;
    private String sceneType;
}
