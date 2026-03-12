package com.semantyca.datanest.service.util;

import com.semantyca.datanest.dto.SoundFragmentDTO;
import com.semantyca.datanest.dto.radio.MessageDTO;
import com.semantyca.datanest.dto.radio.SubmissionDTO;
import com.semantyca.datanest.dto.radiostation.OneTimeStreamRunReqDTO;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@ApplicationScoped
public class ValidationService {
    private static final Logger LOGGER = LoggerFactory.getLogger(ValidationService.class);

    private final Validator validator;

    @Inject
    public ValidationService(Validator validator) {
        this.validator = validator;
    }

    public ValidationResult validateSoundFragmentDTO(String id, SoundFragmentDTO dto) {
        Set<ConstraintViolation<SoundFragmentDTO>> violations = validator.validate(dto);
        Map<String, List<String>> fieldErrors = new HashMap<>();

        if (("new".equalsIgnoreCase(id) || id == null) && (dto.getNewlyUploaded() == null || dto.getNewlyUploaded().isEmpty())) {
            violations = new HashSet<>(violations);
            fieldErrors.computeIfAbsent("newlyUploaded", k -> new ArrayList<>())
                    .add("Music file is required - either provide an existing ID or upload new files");
        }

        for (ConstraintViolation<SoundFragmentDTO> v : violations) {
            String field = v.getPropertyPath().toString();
            fieldErrors.computeIfAbsent(field, k -> new ArrayList<>()).add(v.getMessage());
        }

        if (fieldErrors.isEmpty()) {
            return ValidationResult.success();
        }

        String errorMessage = fieldErrors.entrySet().stream()
                .flatMap(e -> e.getValue().stream().map(msg -> e.getKey() + ": " + msg))
                .collect(Collectors.joining(", "));

        LOGGER.warn("Validation failed for SoundFragmentDTO: {}", errorMessage);
        return ValidationResult.failure(errorMessage, fieldErrors);
    }

    public Uni<ValidationResult> validateSubmissionDTO(SubmissionDTO dto) {
        Set<ConstraintViolation<SubmissionDTO>> violations = validator.validate(dto);

        if (!violations.isEmpty()) {
            String errorMessage = violations.stream()
                    .map(violation -> violation.getPropertyPath() + ": " + violation.getMessage())
                    .collect(Collectors.joining(", "));

            LOGGER.warn("Validation failed for SubmissionDTO: {}", errorMessage);
            return Uni.createFrom().item(ValidationResult.failure(errorMessage));
        }



        return Uni.createFrom().item(ValidationResult.success());
    }

    public Uni<ValidationResult> validateMessageDTO(MessageDTO dto) {
        Set<ConstraintViolation<MessageDTO>> violations = validator.validate(dto);

        if (!violations.isEmpty()) {
            String errorMessage = violations.stream()
                    .map(violation -> violation.getPropertyPath() + ": " + violation.getMessage())
                    .collect(Collectors.joining(", "));

            LOGGER.warn("Validation failed for MessageDTO: {}", errorMessage);
            return Uni.createFrom().item(ValidationResult.failure(errorMessage));
        }



        return Uni.createFrom().item(ValidationResult.success());
    }


}