package com.semantyca.datanest.dto;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;
import java.util.UUID;

@Getter
@Setter
@NoArgsConstructor
public class BulkACLRequestDTO {
    private List<UUID> documentIds;
    private List<RlsActionDTO> actions;
    private String resourceType;
}
