package com.semantyca.datanest.model.cnst;

@Deprecated
public enum ApprovalStatus {
    OPEN(500),
    CANCELLED(501),
    REJECTED_NOT_MEET_GENRE(502),
    REJECTED(503),
    ACCEPTED(505);

    private final int value;

    ApprovalStatus(int value) {
        this.value = value;
    }

    public int value() {
        return value;
    }

    public static ApprovalStatus fromValue(Integer v) {
        if (v == null) return null;
        for (ApprovalStatus p : values()) {
            if (p.value == v) return p;
        }
        return null;
    }
}
