package com.semantyca.datanest.model;

import com.semantyca.core.model.cnst.ChatType;
import com.semantyca.core.model.cnst.SummaryType;
import lombok.Getter;
import lombok.Setter;

import java.time.OffsetDateTime;
import java.util.UUID;

@Getter
@Setter
public class ChatSummary {
    private UUID id;
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
