package com.semantyca.datanest.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.semantyca.core.dto.AbstractDTO;
import lombok.Getter;
import lombok.Setter;

import java.util.HashMap;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
@Setter
@Getter
public class UserAdDTO extends AbstractDTO {
    private long userId;
    private String title;
    private String description;
    private String contacts;
    private Integer archived;
    private Map<String, String> userData = new HashMap<>();
}
