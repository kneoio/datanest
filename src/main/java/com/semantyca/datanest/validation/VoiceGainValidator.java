package com.semantyca.datanest.validation;

import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;

import java.util.Arrays;
import java.util.List;

public class VoiceGainValidator implements ConstraintValidator<ValidVoiceGain, Float> {
    
    private static final List<Float> VALID_GAIN_VALUES = Arrays.asList(
        0.25f,  // -12 dB
        0.5f,   // -6 dB
        0.75f,  // -2.5 dB
        1.0f,   // 0 dB
        1.25f,  // +2 dB
        1.5f,   // +3.5 dB
        2.0f    // +6 dB
    );

    @Override
    public boolean isValid(Float value, ConstraintValidatorContext context) {
        if (value == null) {
            return true;
        }
        
        return VALID_GAIN_VALUES.stream()
            .anyMatch(validValue -> Float.compare(validValue, value) == 0);
    }
}
