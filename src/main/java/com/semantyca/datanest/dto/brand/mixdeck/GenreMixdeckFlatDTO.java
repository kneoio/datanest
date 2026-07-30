package com.semantyca.datanest.dto.brand.mixdeck;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.semantyca.core.model.cnst.LanguageCode;
import com.semantyca.officeframe.dto.GenreDTO;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class GenreMixdeckFlatDTO {
    private String identifier;
    private String name;
    private Integer rank;
    private String color;
    private String fontColor;
    private String parentIdentifier;
    private List<GenreMixdeckFlatDTO> children;

    public static GenreMixdeckFlatDTO from(GenreDTO genre) {
        return from(genre, null);
    }

    private static GenreMixdeckFlatDTO from(GenreDTO genre, String parentIdentifier) {
        if (genre == null) {
            return null;
        }
        GenreMixdeckFlatDTO f = new GenreMixdeckFlatDTO();
        f.setIdentifier(genre.getIdentifier());
        f.setRank(genre.getRank());
        f.setColor(genre.getColor());
        f.setFontColor(genre.getFontColor());
        f.setParentIdentifier(parentIdentifier);
        f.setName(pickName(genre));
        if (genre.getChildren() != null && !genre.getChildren().isEmpty()) {
            f.setChildren(genre.getChildren().stream()
                    .map(child -> from(child, genre.getIdentifier()))
                    .toList());
        }
        return f;
    }

    private static String pickName(GenreDTO genre) {
        if (genre.getLocalizedName() == null || genre.getLocalizedName().isEmpty()) {
            return null;
        }
        if (genre.getLocalizedName().containsKey(LanguageCode.en)) {
            return genre.getLocalizedName().get(LanguageCode.en);
        }
        return genre.getLocalizedName().values().iterator().next();
    }
}
