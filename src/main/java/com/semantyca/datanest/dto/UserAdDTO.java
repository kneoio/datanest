package com.semantyca.datanest.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.semantyca.core.dto.AbstractDTO;
import com.semantyca.mixpla.model.PlayHistory;
import lombok.Getter;
import lombok.Setter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@JsonInclude(JsonInclude.Include.NON_NULL)
@Setter
@Getter
public class UserAdDTO extends AbstractDTO {
    private long userId;
    private UUID brandId;
    private String title;
    private String description;
    private String contacts;
    private Integer archived;
    private Map<String, String> userData = new HashMap<>();
    private List<PlayHistory> playHistory;
}
