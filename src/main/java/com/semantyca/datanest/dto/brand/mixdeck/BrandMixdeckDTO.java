package com.semantyca.datanest.dto.brand.mixdeck;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.semantyca.core.dto.AbstractDTO;
import com.semantyca.core.dto.validation.ValidCountry;
import com.semantyca.core.dto.validation.ValidLocalizedName;
import com.semantyca.core.model.cnst.LanguageCode;
import com.semantyca.datanest.dto.brand.AiOverridingDTO;
import com.semantyca.datanest.dto.brand.ProfileOverridingDTO;
import com.semantyca.datanest.model.cnst.ScriptMode;
import com.semantyca.mixpla.model.brand.StreamHistoryEntry;
import com.semantyca.mixpla.model.brand.StreamingOptions;
import com.semantyca.mixpla.model.cnst.ChatFeatureFlag;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Mixdeck brand form DTO — slugs/identifiers only; no document UUIDs.
 */
@Setter
@Getter
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties({"id"})
public class BrandMixdeckDTO extends AbstractDTO {
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
    private URL hlsUrlAAC;
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
    private int publicBrand;
    private String aiAgentSlug;
    private String profileSlug;
    private boolean aiOverridingEnabled;
    private boolean profileOverridingEnabled;
    private AiOverridingDTO aiOverriding;
    private ProfileOverridingDTO profileOverriding;
    private List<BrandScriptEntryMixdeckDTO> scripts;
    private String customScriptSlug;
    private ScriptMode scriptMode = ScriptMode.PREDEFINED;
    private StreamingOptions streamingOptions;
    private StreamHistoryEntry lastStreamHistoryEntry;
    private CustomScriptMixdeckDTO customScript;
    private OwnerMixdeckDTO owner;
    private List<String> labels;
    private List<String> genres;
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    private List<FileMixdeckDTO> logoFiles;
    private boolean skipScriptValidation = false;
    private Map<ChatFeatureFlag, Boolean> chatFeatureFlags = new HashMap<>();

    @JsonIgnore
    @AssertTrue(message = "Custom script must have at least one scene, and each scene must have at least one action")
    public boolean isCustomScriptValid() {
        if (skipScriptValidation) {
            return true;
        }
        if (!ScriptMode.CUSTOM.equals(scriptMode)) {
            return true;
        }
        if (customScriptSlug != null && !customScriptSlug.isBlank()) {
            return true;
        }
        if (customScript == null || customScript.getScenes() == null || customScript.getScenes().isEmpty()) {
            return false;
        }
        return customScript.getScenes().stream()
                .anyMatch(scene -> scene.getActions() != null && !scene.getActions().isEmpty());
    }
}
