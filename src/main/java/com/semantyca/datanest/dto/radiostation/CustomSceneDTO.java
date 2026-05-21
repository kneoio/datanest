package com.semantyca.datanest.dto.radiostation;

import com.fasterxml.jackson.annotation.JsonInclude;
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
    private List<SceneActionDTO> actions;
}
