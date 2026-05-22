package com.semantyca.datanest.dto.radiostation;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;
import java.util.UUID;

@Setter
@Getter
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SceneActionDTO {
    private String name;
    private boolean predefined;
    private UUID promptId;
    private String instruction;
    private List<String> contextVars;
}
