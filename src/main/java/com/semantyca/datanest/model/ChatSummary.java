package com.semantyca.datanest.model;

import com.semantyca.core.model.cnst.ChatType;
import com.semantyca.datanest.model.cnst.SummaryType;
import lombok.Getter;
import lombok.Setter;

import java.time.LocalDateTime;
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
    private LocalDateTime periodStart;
    private LocalDateTime periodEnd;
    private LocalDateTime createdAt;
}
