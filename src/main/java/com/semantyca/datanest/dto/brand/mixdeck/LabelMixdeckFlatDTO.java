package com.semantyca.datanest.dto.brand.mixdeck;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.semantyca.core.model.cnst.LanguageCode;
import com.semantyca.officeframe.dto.LabelDTO;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class LabelMixdeckFlatDTO {
    private String identifier;
    private String name;
    private String color;
    private String fontColor;
    private String category;

    public static LabelMixdeckFlatDTO from(LabelDTO label) {
        if (label == null) {
            return null;
        }
        LabelMixdeckFlatDTO dto = new LabelMixdeckFlatDTO();
        dto.setIdentifier(label.getIdentifier());
        dto.setColor(label.getColor());
        dto.setFontColor(label.getFontColor());
        dto.setCategory(label.getCategory());
        dto.setName(pickName(label));
        return dto;
    }

    private static String pickName(LabelDTO label) {
        if (label.getLocalizedName() == null || label.getLocalizedName().isEmpty()) {
            return null;
        }
        if (label.getLocalizedName().containsKey(LanguageCode.en)) {
            return label.getLocalizedName().get(LanguageCode.en);
        }
        return label.getLocalizedName().values().iterator().next();
    }
}
