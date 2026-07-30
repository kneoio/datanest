package com.semantyca.datanest.dto.brand.mixdeck;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.Map;

@Setter
@Getter
@NoArgsConstructor
public class BrandScriptEntryMixdeckDTO {
    private String slugName;
    private Map<String, Object> userVariables;
}
