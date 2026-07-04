package com.semantyca.datanest.model.cnst;

// The single status enum for the shared-sound-fragment "received inbox" — used uniformly for
// both station-to-station shares and artist contributions (which are themselves created as a
// PENDING share from the submitter to the target station; see SHARING_WORKFLOW.md in
// repository/soundfragment). Values reuse the numeric codes already written by existing rows
// (500/501/506) so no data migration is needed — this is a cleanup of the enum surface (was six
// overlapping values: OPEN/CANCELLED/REJECTED_NOT_MEET_GENRE/REJECTED/ACCEPTED/PENDING), not a
// change to what's stored.
public enum ApprovalStatus {
    PENDING(506),
    ACCEPTED(500),
    REJECTED(501);

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
