package com.semantyca.datanest.dto.brand.mixdeck;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;

@Setter
@Getter
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class OwnerMixdeckDTO {
    private String name;
    private String email;
    private boolean exposeWhileSharing;
    private boolean actionDebugEnabled;
    private List<OwnerMixdeckDTO> coOwners;
}
