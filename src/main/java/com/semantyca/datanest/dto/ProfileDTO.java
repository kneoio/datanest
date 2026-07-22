package com.semantyca.datanest.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.semantyca.core.dto.AbstractDTO;
import com.semantyca.core.dto.rls.RlsActionDTO;
import jakarta.validation.constraints.NotBlank;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

@Setter
@Getter
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ProfileDTO extends AbstractDTO {
    @NotBlank
    private String name;
    private String description;
    private boolean explicitContent;
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    private List<RlsActionDTO> rlsActions = new ArrayList<>();
}