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
public class CustomSceneActionDTO {
    private String name;
    private String instruction;
    private List<String> contextVars;
}
