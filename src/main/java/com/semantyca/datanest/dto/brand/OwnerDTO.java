package com.semantyca.datanest.dto.brand;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class OwnerDTO {
    private Long userId;
    private String name;
    private String email;
    private boolean exposeWhileSharing;
    private boolean actionDebugEnabled;
}