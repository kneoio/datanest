package com.semantyca.datanest.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import jakarta.validation.constraints.AssertTrue;
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
    private boolean stayIncognito = false;

    @AssertTrue(message = "At least one of addTargetBrandIds or removeTargetBrandIds must be provided")
    public boolean isPatchNotEmpty() {
        return (addTargetBrandIds != null && !addTargetBrandIds.isEmpty())
                || (removeTargetBrandIds != null && !removeTargetBrandIds.isEmpty());
    }
}
