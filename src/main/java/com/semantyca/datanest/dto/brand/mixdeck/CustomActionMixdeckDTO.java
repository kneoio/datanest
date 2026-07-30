package com.semantyca.datanest.dto.brand.mixdeck;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;

@Setter
@Getter
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class CustomActionMixdeckDTO {
    private String type;
    private String name;
    private String instruction;
    private String actionSlug;
    private List<String> contextVars;
}
