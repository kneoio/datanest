package com.semantyca.datanest.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.semantyca.core.dto.AbstractDTO;
import lombok.Getter;
import lombok.Setter;

@JsonInclude(JsonInclude.Include.NON_NULL)
@Setter
@Getter
public class DraftDTO extends AbstractDTO {
    private String title;
    private String content;
    private String description;
    private Integer archived;
    private boolean enabled;
    private double version;
}
