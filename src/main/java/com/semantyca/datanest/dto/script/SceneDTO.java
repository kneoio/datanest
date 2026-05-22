package com.semantyca.datanest.dto.script;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.semantyca.core.dto.AbstractDTO;
import com.semantyca.core.dto.rls.RlsActionDTO;
import com.semantyca.datanest.dto.StagePlaylistDTO;
import lombok.Getter;
import lombok.Setter;

import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Setter
@Getter
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SceneDTO extends AbstractDTO {
    private UUID scriptId;
    private String title;
    private String scriptTitle;
    private double talkativity;
    private double podcastMode;
    private List<ScenePromptDTO> prompts;
    private StagePlaylistDTO stagePlaylist;
    private List<LocalTime> startTime;
    private int durationSeconds;
    private int seqNum;
    private List<Integer> weekdays;
    private boolean oneTimeRun;
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    private List<RlsActionDTO> rlsActions = new ArrayList<>();
}
