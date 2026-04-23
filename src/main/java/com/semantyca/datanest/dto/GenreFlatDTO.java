package com.semantyca.datanest.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.semantyca.core.model.cnst.LanguageCode;
import com.semantyca.officeframe.dto.GenreDTO;
import lombok.Getter;
import lombok.Setter;

import java.util.List;
import java.util.UUID;

@Getter
@Setter
@JsonInclude(JsonInclude.Include.NON_NULL)
public class GenreFlatDTO {
    private UUID id;
    private String name;
    private String identifier;
    private Integer rank;
    private String color;
    private String fontColor;
    private UUID parent;
    private List<GenreFlatDTO> children;

    public static GenreFlatDTO from(GenreDTO genre) {
        if (genre == null) {
            return null;
        }
        GenreFlatDTO f = new GenreFlatDTO();
        f.setId(genre.getId());
        f.setIdentifier(genre.getIdentifier());
        f.setRank(genre.getRank());
        f.setColor(genre.getColor());
        f.setFontColor(genre.getFontColor());
        f.setParent(genre.getParent());
        f.setName(pickName(genre));
        if (genre.getChildren() != null && !genre.getChildren().isEmpty()) {
            f.setChildren(genre.getChildren().stream().map(GenreFlatDTO::from).toList());
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
