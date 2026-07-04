package com.semantyca.datanest.dto.event;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.semantyca.core.dto.AbstractDTO;
import com.semantyca.core.dto.rls.RlsActionDTO;
import com.semantyca.core.dto.scheduler.ScheduleDTO;
import com.semantyca.datanest.dto.PlaylistRequestDTO;
import com.semantyca.datanest.dto.script.ScenePromptDTO;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

import java.util.ArrayList;
import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
@Setter
@Getter
@SuperBuilder
@NoArgsConstructor
public class EventDTO extends AbstractDTO {
    //private String brand;
    private String brandId;
    @Pattern(regexp = "^[A-Za-z_]+/[A-Za-z_]+(?:/[A-Za-z_]+)?$", message = "Invalid timezone format")
    private String timeZone;
    private String type;
    @NotNull(message = "Description is required")
    private String description;
    private ScheduleDTO schedule;
    private String priority;
    private List<ScenePromptDTO> actions;
    private PlaylistRequestDTO stagePlaylist;
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @Builder.Default
    private List<RlsActionDTO> rlsActions = new ArrayList<>();

}