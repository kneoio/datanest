package com.semantyca.datanest.dto.brand.mixdeck;

import com.fasterxml.jackson.annotation.JsonInclude;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.LocalTime;
import java.util.List;

@Setter
@Getter
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class CustomSceneMixdeckDTO {
    private String name;
    @NotNull
    private LocalTime startTime;
    private boolean allowJingles;
    private boolean allowAds;
    private double talkativity;
    private PlaylistRequestMixdeckDTO stagePlaylist;
    private List<CustomActionMixdeckDTO> actions;
}
