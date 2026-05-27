package com.semantyca.datanest.dto.script;

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
public class CustomActionDTO {
    public static final List<String> AVAILABLE_CONTEXT_VARS = List.of(
            "songTitle", "songArtist", "genre", "country", "stationBrand", "djName", "timeContext", "labels", "listeners"
    );

    private String type;
    private String name;
    private String instruction;
    private UUID actionId;
    private List<String> contextVars;
}
