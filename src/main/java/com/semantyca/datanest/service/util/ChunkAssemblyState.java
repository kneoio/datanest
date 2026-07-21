package com.semantyca.datanest.service.util;

import java.util.concurrent.atomic.AtomicInteger;

final class ChunkAssemblyState {
    final int totalChunks;
    final AtomicInteger receivedCount = new AtomicInteger(0);
    final String originalFileName;
    final String safeFileName;
    final String batchId;
    final String entityId; // null = bulk mode; UUID string = single-entity mode

    ChunkAssemblyState(int totalChunks, String originalFileName, String safeFileName, String batchId, String entityId) {
        this.totalChunks = totalChunks;
        this.originalFileName = originalFileName;
        this.safeFileName = safeFileName;
        this.batchId = batchId;
        this.entityId = entityId;
    }
}
