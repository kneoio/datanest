package com.semantyca.datanest.model.brand;



import com.semantyca.datanest.model.cnst.StreamStatus;

import java.time.LocalDateTime;

public record StatusChangeRecord(LocalDateTime timestamp, StreamStatus oldStatus,
                                 StreamStatus newStatus) {
}
