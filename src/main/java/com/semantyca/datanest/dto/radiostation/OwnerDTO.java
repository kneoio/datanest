package com.semantyca.datanest.dto.radiostation;

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