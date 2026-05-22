package com.semantyca.datanest.dto.radiostation;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.semantyca.mixpla.model.cnst.WayOfSourcing;
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
    private LocalTime startTime;
    private boolean allowJingles;
    private boolean allowAds;
    private int talkActivity;
    private WayOfSourcing songsMode;
    private List<String> songsLabels;
    private List<SceneActionDTO> actions;
}
