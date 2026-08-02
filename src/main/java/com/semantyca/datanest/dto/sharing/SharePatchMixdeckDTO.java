package com.semantyca.datanest.dto.sharing;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonInclude;
import jakarta.validation.constraints.AssertTrue;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

@Setter
@Getter
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SharePatchMixdeckDTO {
    @JsonAlias("addTargetBrandIds")
    private List<String> addTargetBrandSlugs = new ArrayList<>();
    @JsonAlias("removeTargetBrandIds")
    private List<String> removeTargetBrandSlugs = new ArrayList<>();
    private boolean stayIncognito = false;
    private boolean notifyOnPlay = false;

    @AssertTrue(message = "At least one of addTargetBrandSlugs or removeTargetBrandSlugs must be provided")
    public boolean isPatchNotEmpty() {
        return (addTargetBrandSlugs != null && !addTargetBrandSlugs.isEmpty())
                || (removeTargetBrandSlugs != null && !removeTargetBrandSlugs.isEmpty());
    }
}
