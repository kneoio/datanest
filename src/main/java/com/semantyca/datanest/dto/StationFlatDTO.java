package com.semantyca.datanest.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.semantyca.core.model.cnst.LanguageCode;
import com.semantyca.datanest.dto.brand.BrandDTO;
import lombok.Getter;
import lombok.Setter;

import java.util.List;
import java.util.Map;
import java.util.UUID;

@Getter
@Setter
@JsonInclude(JsonInclude.Include.NON_NULL)
public class StationFlatDTO {
    private UUID id;
    private String slugName;
    private String name;
    private String country;
    private String description;
    private String color;
    private List<GenreRefDTO> genres;

    @Getter
    @Setter
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class GenreRefDTO {
        private String slug;
        private String name;
    }

    public static StationFlatDTO from(BrandDTO dto, Map<UUID, GenreFlatDTO> genreMap) {
        StationFlatDTO f = new StationFlatDTO();
        f.setId(dto.getId());
        f.setSlugName(dto.getSlugName());
        f.setCountry(dto.getCountry());
        f.setDescription(dto.getDescription());
        f.setColor(dto.getColor());
        if (dto.getLocalizedName() != null) {
            f.setName(dto.getLocalizedName().getOrDefault(LanguageCode.en,
                    dto.getLocalizedName().values().stream().findFirst().orElse(null)));
        }
        if (dto.getGenres() != null && !dto.getGenres().isEmpty()) {
            f.setGenres(dto.getGenres().stream()
                    .map(genreMap::get)
                    .filter(g -> g != null)
                    .map(g -> {
                        GenreRefDTO ref = new GenreRefDTO();
                        ref.setSlug(g.getIdentifier());
                        ref.setName(g.getName());
                        return ref;
                    })
                    .toList());
        }
        return f;
    }
}
