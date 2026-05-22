package com.semantyca.datanest.dto;

import lombok.Getter;
import lombok.Setter;

import java.util.UUID;

@Getter
@Setter
public class PromptOptionDTO {
    UUID id;
    String name;
}
