package com.semantyca.model.brand;



import com.semantyca.model.cnst.StreamStatus;

import java.time.LocalDateTime;

public record StatusChangeRecord(LocalDateTime timestamp, StreamStatus oldStatus,
                                 StreamStatus newStatus) {
}
