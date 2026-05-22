package com.semantyca.datanest.dto.brand;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class OwnerDTO {
    private long userId;
    private String name;
    private String email;
    private boolean exposeWhileSharing;
}