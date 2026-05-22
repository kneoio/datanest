package com.semantyca.datanest.dto.script;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.semantyca.datanest.dto.StagePlaylistDTO;
import com.semantyca.mixpla.model.cnst.WayOfSourcing;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.LocalTime;
import java.util.List;
import java.util.UUID;

@Setter
@Getter
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class CustomSceneDTO {
    private LocalTime startTime;
    private boolean allowJingles;
    private boolean allowAds;
    private int talkativity;
    @Deprecated
    private WayOfSourcing songsMode;
    private StagePlaylistDTO stagePlaylist;
    @Deprecated
    private List<UUID> songsLabels;
    private List<ScenePromptDTO> introPrompts;
    private List<CustomActionDTO> actions;
}
