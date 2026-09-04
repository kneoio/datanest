package com.semantyca.datanest.dto.brand.mixdeck;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.semantyca.core.dto.AbstractDTO;
import com.semantyca.core.dto.validation.ValidLocalizedName;
import com.semantyca.core.model.cnst.LanguageCode;
import jakarta.validation.constraints.Email;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Mixdeck listener DTO — slugName (user login) only; no document UUID exposure.
 */
@Setter
@Getter
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties({"id"})
public class ListenerMixdeckDTO extends AbstractDTO {
    @Email
    private String email;
    private String slugName;
    @ValidLocalizedName(
            minLength = 1,
            maxLength = 255,
            allowEmptyMap = false,
            requireDefaultLanguage = true,
            defaultLanguage = LanguageCode.en,
            message = "Invalid localized name format"
    )
    private EnumMap<LanguageCode, String> localizedName = new EnumMap<>(LanguageCode.class);
    private EnumMap<LanguageCode, Set<String>> nickName = new EnumMap<>(LanguageCode.class);
    private Map<String, String> userData = new HashMap<>();
    private List<String> labels;
}
