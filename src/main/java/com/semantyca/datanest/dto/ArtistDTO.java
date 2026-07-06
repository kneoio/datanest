package com.semantyca.datanest.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.semantyca.core.dto.AbstractDTO;
import com.semantyca.core.dto.rls.RlsActionDTO;
import com.semantyca.core.model.UserData;
import com.semantyca.core.model.cnst.LanguageCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

@Setter
@Getter
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ArtistDTO extends AbstractDTO {
    private EnumMap<LanguageCode, String> localizedName;
    private String country;
    private List<String> styles;
    private String description;
    private Map<String, String> links;
    private UserData userData;
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    private List<RlsActionDTO> rlsActions = new ArrayList<>();
}
