package com.semantyca.datanest.dto.brand;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.semantyca.core.model.cnst.LanguageCode;
import com.semantyca.mixpla.model.cnst.SubmissionPolicy;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.net.URL;
import java.time.ZonedDateTime;
import java.util.EnumMap;
import java.util.UUID;

@Setter
@Getter
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class BrandPublicFlatDTO {
    private UUID id;
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "dd.MM.yyyy HH:mm")
    private ZonedDateTime regDate;
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "dd.MM.yyyy HH:mm")
    private ZonedDateTime lastModifiedDate;
    private EnumMap<LanguageCode, String> localizedName;
    private String slugName;
    private String country;
    private String color;
    private String description;
    private String titleFont;
    private long bitRate;
    private double popularityRate;
    private int publicBrand;
    private SubmissionPolicy oneTimeStreamPolicy;
    private SubmissionPolicy submissionPolicy;
    private SubmissionPolicy messagingPolicy;
    private URL hlsUrl;
    private URL mp3Url;
    private URL mixplaUrl;
}
