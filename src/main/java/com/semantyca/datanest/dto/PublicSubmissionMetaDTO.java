package com.semantyca.datanest.dto;

// Carries the extra fields the public/anonymous submission form collects that don't fit
// UploadFileDTO, so they can ride through FileUploadService's chunk-assembly pipeline to
// SoundFragmentService.createFromBulkUpload without piling up individual parameters.
public record PublicSubmissionMetaDTO(String submitterEmail, String artistName, String description, boolean notifyOnPlay) {
}
