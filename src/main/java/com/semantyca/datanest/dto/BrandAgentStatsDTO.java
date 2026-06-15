package com.semantyca.datanest.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.semantyca.mixpla.model.cnst.StreamType;
import com.semantyca.officeframe.model.cnst.CountryCode;
import lombok.Getter;
import lombok.Setter;

import java.time.OffsetDateTime;

@Getter
@Setter
@JsonInclude(JsonInclude.Include.NON_NULL)
public class BrandAgentStatsDTO {
    private Integer id;
    private String stationName;
    private String userAgent;
    private String ipAddress;
    private CountryCode countryCode;
    private Long accessCount;
    private StreamType streamType;
    private OffsetDateTime lastAccessTime;
}
