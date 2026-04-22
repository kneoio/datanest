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
        LabelFlatDTO r = new LabelFlatDTO();
        r.setId(label.getId());
        r.setIdentifier(label.getIdentifier());
        r.setColor(label.getColor());
        r.setFontColor(label.getFontColor());
        r.setCategory(label.getCategory());
        r.setName(pickName(label));
        return r;
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
