package com.semantyca.datanest.dto.script;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.semantyca.datanest.dto.PlaylistRequestDTO;
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
public class CustomSceneDTO {
    private String name;
    @NotNull
    private LocalTime startTime;
    private boolean allowJingles;
    private boolean allowAds;
    private double talkativity;
    private PlaylistRequestDTO stagePlaylist;
    private List<ScenePromptDTO> introPrompts;
    private List<CustomActionDTO> actions;
}
