package com.semantyca.datanest.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import jakarta.validation.constraints.NotBlank;
import lombok.Getter;
import lombok.Setter;

import java.util.UUID;

@Getter
@Setter
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ProfileFlatDTO {
    private UUID id;
    @NotBlank
    private String name;
    private String description;
    private boolean explicitContent;
}
