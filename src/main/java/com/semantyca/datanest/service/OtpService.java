package com.semantyca.datanest.service;

import io.quarkus.mailer.Mail;
import io.quarkus.mailer.reactive.ReactiveMailer;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.SecureRandom;
import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;

@ApplicationScoped
public class OtpService {
    private static final Logger LOGGER = LoggerFactory.getLogger(OtpService.class);
    private static final long OTP_TTL_SECONDS = 600;

    private final ReactiveMailer mailer;
    private final ConcurrentHashMap<String, OtpEntry> store = new ConcurrentHashMap<>();
    private final SecureRandom rng = new SecureRandom();

    @Inject
    public OtpService(ReactiveMailer mailer) {
        this.mailer = mailer;
    }

    public Uni<Void> sendOtp(String email) {
        String code = generateCode();
        store.put(email.toLowerCase(), new OtpEntry(code, Instant.now().plusSeconds(OTP_TTL_SECONDS)));
        LOGGER.info("OTP generated for {}", email);
        return mailer.send(Mail.withText(
                email,
                "Your upload confirmation code",
                "Your confirmation code is: " + code + "\n\nIt expires in 10 minutes."
        ));
    }

    public boolean verify(String email, String code) {
        OtpEntry entry = store.get(email.toLowerCase());
        if (entry == null || Instant.now().isAfter(entry.expiry())) {
            store.remove(email.toLowerCase());
            return false;
        }
        if (!entry.code().equals(code)) {
            return false;
        }
        store.remove(email.toLowerCase());
        return true;
    }

    private String generateCode() {
        int n = rng.nextInt(1_000_000);
        return String.format("%06d", n);
    }

    private record OtpEntry(String code, Instant expiry) {}
}
