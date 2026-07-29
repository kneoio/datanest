package com.semantyca.datanest.dto.script;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.Map;

@Setter
@Getter
@NoArgsConstructor
public class BrandScriptEntryDTO {
    private String slugName;
    private Map<String, Object> userVariables;
}
