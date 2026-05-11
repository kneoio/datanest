package com.semantyca.datanest.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Setter
@Getter
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SharedSoundFragmentPatchDTO {
    private List<UUID> addTargetBrandIds = new ArrayList<>();
    private List<UUID> removeTargetBrandIds = new ArrayList<>();
}
