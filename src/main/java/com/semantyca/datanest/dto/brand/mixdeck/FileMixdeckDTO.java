package com.semantyca.datanest.dto.brand.mixdeck;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.semantyca.core.model.FileMetadata;
import com.semantyca.core.model.cnst.FileType;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Setter
@Getter
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class FileMixdeckDTO {
    private String slugName;
    private String fileOriginalName;
    private String mimeType;
    private FileType fileType;
    private Long contentLength;

    public static FileMixdeckDTO from(FileMetadata file) {
        if (file == null) {
            return null;
        }
        FileMixdeckDTO dto = new FileMixdeckDTO();
        dto.setSlugName(file.getSlugName());
        dto.setFileOriginalName(file.getFileOriginalName());
        dto.setMimeType(file.getMimeType());
        dto.setFileType(file.getFileType());
        dto.setContentLength(file.getContentLength());
        return dto;
    }
}
