package com.semantyca.datanest.service;

import com.semantyca.datanest.config.DatanestConfig;
import io.quarkus.mailer.Mail;
import io.quarkus.mailer.reactive.ReactiveMailer;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;


import java.security.SecureRandom;
import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;

@ApplicationScoped
public class OtpService {
    private static final Logger LOGGER = Logger.getLogger(OtpService.class);
    private static final long OTP_TTL_SECONDS = 600;

    private final DatanestConfig config;
    private final ReactiveMailer mailer;
    private final ConcurrentHashMap<String, OtpEntry> store = new ConcurrentHashMap<>();
    private final SecureRandom rng = new SecureRandom();

    @Inject
    public OtpService(DatanestConfig config, ReactiveMailer mailer) {
        this.config = config;
        this.mailer = mailer;
    }

    public Uni<Void> sendOtp(String email) {
        String code = generateCode();
        store.put(email.toLowerCase(), new OtpEntry(code, Instant.now().plusSeconds(OTP_TTL_SECONDS)));
        LOGGER.infof("OTP generated for %s", email);
        return mailer.send(Mail.withText(
                email,
                "Your upload confirmation code",
                "Your confirmation code is: " + code + "\n\nIt expires in 10 minutes."
        ));
    }

    public boolean isVerifyFail(String email, String code) {
        if (config.getOtpTestBypassEmail().equalsIgnoreCase(email) && config.getOtpTestBypassCode().equals(code)) {
            return false;
        }
        OtpEntry entry = store.get(email.toLowerCase());
        if (entry == null || Instant.now().isAfter(entry.expiry())) {
            store.remove(email.toLowerCase());
            return true;
        }
        return !entry.code().equals(code);
    }

    private String generateCode() {
        int n = rng.nextInt(1_000_000);
        return String.format("%06d", n);
    }

    private record OtpEntry(String code, Instant expiry) {}
}
