package com.semantyca.datanest.dto.sharing;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.semantyca.core.model.cnst.LanguageCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.EnumMap;
import java.util.List;

@Setter
@Getter
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ShareTargetBrandDTO {
    private String slugName;
    private EnumMap<LanguageCode, String> localizedName;
    private List<String> genres;
}
