package com.semantyca.datanest.util;

public final class DocumentIds {

    private DocumentIds() {
    }

    public static boolean isNewDocumentId(String id) {
        return id == null || id.isBlank() || "new".equalsIgnoreCase(id);
    }
}
