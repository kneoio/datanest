package com.semantyca.datanest.dto.brand;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.semantyca.core.dto.AbstractDTO;
import com.semantyca.core.dto.rls.RlsActionDTO;
import com.semantyca.core.dto.validation.ValidCountry;
import com.semantyca.core.dto.validation.ValidLocalizedName;
import com.semantyca.core.model.cnst.LanguageCode;
import com.semantyca.datanest.dto.script.BrandScriptEntryDTO;
import com.semantyca.datanest.dto.script.CustomScriptDTO;
import com.semantyca.datanest.model.cnst.ScriptMode;
import com.semantyca.mixpla.model.cnst.SubmissionPolicy;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.net.URL;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.UUID;

@Setter
@Getter
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class BrandDTO extends AbstractDTO {
    @NotNull(message = "Localized name is required")
    @ValidLocalizedName(
            minLength = 1,
            maxLength = 255,
            allowEmptyMap = false,
            requireDefaultLanguage = true,
            defaultLanguage = LanguageCode.en,
            message = "Invalid localized name format"
    )
    private EnumMap<LanguageCode, String> localizedName = new EnumMap<>(LanguageCode.class);
    private String slugName;
    @NotNull(message = "Country is required")
    @NotBlank(message = "Country cannot be empty")
    @ValidCountry(message = "It is not available for the country")
    private String country;
    private URL hlsUrl;
    private URL iceCastUrl;
    private URL mp3Url;
    private URL mixplaUrl;
    @NotBlank
    @Pattern(regexp = "^[A-Za-z]+/[A-Za-z_]+$", message = "Invalid timezone format")
    private String timeZone;
    private String color;
    private String description;
    private String titleFont;
    private long bitRate;
    private double popularityRate;
    private SubmissionPolicy oneTimeStreamPolicy = SubmissionPolicy.NOT_ALLOWED;
    private SubmissionPolicy submissionPolicy = SubmissionPolicy.NOT_ALLOWED;
    private SubmissionPolicy messagingPolicy = SubmissionPolicy.REVIEW_REQUIRED;
    private Integer isTemporary = 0;
    private int publicBrand;
    private UUID aiAgentId;
    private UUID profileId;
    private boolean aiOverridingEnabled;
    private boolean profileOverridingEnabled;
    private AiOverridingDTO aiOverriding;
    private ProfileOverridingDTO profileOverriding;
    private List<BrandScriptEntryDTO> scripts;
    private ScriptMode scriptMode = ScriptMode.PREDEFINED;
    private CustomScriptDTO customScript;
    private OwnerDTO owner;
    private List<UUID> labels;
    private List<UUID> genres;
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    private List<RlsActionDTO> rlsActions = new ArrayList<>();

    @JsonIgnore
    @AssertTrue(message = "Custom script must have at least one scene, and each scene must have at least one prompt or action")
    public boolean isCustomScriptValid() {
        if (!ScriptMode.CUSTOM.equals(scriptMode)) {
            return true;
        }
        if (customScript == null || customScript.getScenes() == null || customScript.getScenes().isEmpty()) {
            return false;
        }
        return customScript.getScenes().stream().anyMatch(scene ->
                (scene.getIntroPrompts() != null && !scene.getIntroPrompts().isEmpty()) ||
                (scene.getActions() != null && !scene.getActions().isEmpty())
        );
    }

}