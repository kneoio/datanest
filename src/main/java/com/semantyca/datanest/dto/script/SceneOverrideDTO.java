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
public class SceneOverrideDTO extends AbstractDTO {
    private String title;
    private UUID sceneId;
    private List<LocalTime> startTime;
    private List<Object> actionsData;
    private StagePlaylistDTO stagePlaylist;
    private List<Integer> weekdays;
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    private List<RlsActionDTO> rlsActions = new ArrayList<>();
}
