package com.semantyca.datanest.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.semantyca.core.dto.AbstractDTO;
import com.semantyca.core.model.cnst.ChatType;
import com.semantyca.core.model.cnst.SummaryType;
import lombok.Getter;
import lombok.Setter;

import java.time.OffsetDateTime;

@Getter
@Setter
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ChatSummaryDTO extends AbstractDTO {
    private String brandName;
    private SummaryType summaryType;
    private Long userId;
    private ChatType chatType;
    private String summary;
    private Integer messageCount;
    private OffsetDateTime periodStart;
    private OffsetDateTime periodEnd;
    private OffsetDateTime createdAt;
}
