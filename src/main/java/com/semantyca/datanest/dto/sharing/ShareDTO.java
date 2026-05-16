package com.semantyca.datanest.dto.sharing;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Setter
@Getter
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ShareDTO {
    private String targetBrand;
    private Integer status;
    private boolean shared;
}
