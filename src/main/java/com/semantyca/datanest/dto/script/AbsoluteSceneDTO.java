package com.semantyca.datanest.dto.script;

import jakarta.validation.constraints.NotEmpty;
import lombok.Getter;
import lombok.Setter;

import java.time.LocalTime;
import java.util.List;

@Setter
@Getter
public class AbsoluteSceneDTO extends AbstractSceneDTO {
    @NotEmpty
    private List<LocalTime> startTime;
    @NotEmpty
    private List<Integer> weekdays;
}
