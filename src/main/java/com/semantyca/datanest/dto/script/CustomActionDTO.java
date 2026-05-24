package com.semantyca.datanest.dto.script;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;

@Setter
@Getter
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class CustomActionDTO {
    public static final List<String> AVAILABLE_CONTEXT_VARS = List.of(
            "songTitle", "songArtist", "description", "genre", "country", "stationBrand"
    );

    private String name;
    private String instruction;
    private List<String> contextVars;
}
