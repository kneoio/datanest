package com.semantyca.datanest.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.semantyca.core.model.cnst.LanguageCode;
import com.semantyca.officeframe.dto.LabelDTO;
import lombok.Getter;
import lombok.Setter;

import java.util.UUID;

@Getter
@Setter
@JsonInclude(JsonInclude.Include.NON_NULL)
public class LabelFlatDTO {
    private UUID id;
    private String name;
    private String identifier;
    private String color;
    private String fontColor;
    private String category;

    public static LabelFlatDTO from(LabelDTO label) {
        if (label == null) {
            return null;
        }
        LabelFlatDTO dto = new LabelFlatDTO();
        dto.setId(label.getId());
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
