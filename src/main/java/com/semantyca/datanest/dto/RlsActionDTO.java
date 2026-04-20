package com.semantyca.datanest.dto;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
public class RlsActionDTO {
    private RlsActionType action;
    private long userId;
    private boolean canEdit;
    private boolean canDelete;
}
