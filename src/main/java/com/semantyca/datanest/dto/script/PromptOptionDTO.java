package com.semantyca.datanest.dto.script;

import com.semantyca.core.model.cnst.LanguageCode;
import lombok.Getter;
import lombok.Setter;

import java.util.EnumMap;
import java.util.UUID;

@Getter
@Setter
public class PromptOptionDTO {
    UUID id;
    String name;
    EnumMap<LanguageCode, String> localizedOptionName = new EnumMap<>(LanguageCode.class);
}
