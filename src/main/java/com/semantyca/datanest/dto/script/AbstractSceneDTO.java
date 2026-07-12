package com.semantyca.datanest.dto.script;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.semantyca.core.dto.AbstractDTO;
import com.semantyca.core.dto.rls.RlsActionDTO;
import com.semantyca.datanest.dto.PlaylistRequestDTO;
import com.semantyca.mixpla.model.cnst.SceneTimingMode;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Setter
@Getter
@JsonInclude(JsonInclude.Include.NON_NULL)
public abstract class AbstractSceneDTO extends AbstractDTO {
    @NotNull
    private SceneTimingMode timingMode;
    private UUID scriptId;
    private String title;
    private String scriptTitle;
    private double talkativity;
    private double podcastMode;
    private List<ScenePromptDTO> prompts;
    private List<CustomActionDTO> actions;
    private PlaylistRequestDTO playlistRequest;
    private boolean allowJingles;
    private boolean allowAds;
    private boolean oneTimeRun;
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    private List<RlsActionDTO> rlsActions = new ArrayList<>();
}
