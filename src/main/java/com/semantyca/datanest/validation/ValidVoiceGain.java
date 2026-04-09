package com.semantyca.datanest.validation;

import jakarta.validation.Constraint;
import jakarta.validation.Payload;

import java.lang.annotation.*;

@Target({ElementType.FIELD, ElementType.PARAMETER})
@Retention(RetentionPolicy.RUNTIME)
@Constraint(validatedBy = VoiceGainValidator.class)
@Documented
public @interface ValidVoiceGain {
    String message() default "Invalid gain value. Must be one of: 0.25 (-12 dB), 0.5 (-6 dB), 0.75 (-2.5 dB), 1.0 (0 dB), 1.25 (+2 dB), 1.5 (+3.5 dB), 2.0 (+6 dB)";
    
    Class<?>[] groups() default {};
    
    Class<? extends Payload>[] payload() default {};
}
