package com.semantyca.datanest.dto.script;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class RelativeSceneDTO extends AbstractSceneDTO {
    private int seqNum;
    private int durationSeconds;
}
