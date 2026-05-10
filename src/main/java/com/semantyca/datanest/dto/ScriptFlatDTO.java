package com.semantyca.datanest.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Getter;
import lombok.Setter;

import java.util.List;
import java.util.UUID;

@Getter
@Setter
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ScriptFlatDTO {
    private UUID id;
    private String name;
    private String slugName;
    private String description;
    private String languageTag;
    private String timingMode;
    private boolean custom;
    private int accessLevel;
    private List<LabelFlatDTO> tags;
}
