package com.semantyca.datanest.service;

import com.semantyca.datanest.config.DatanestConfig;
import io.quarkus.mailer.Mail;
import io.quarkus.mailer.reactive.ReactiveMailer;
import io.quarkus.runtime.LaunchMode;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;


import java.security.SecureRandom;
import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@ApplicationScoped
public class OtpService {
    private static final Logger LOGGER = Logger.getLogger(OtpService.class);
    private static final long OTP_TTL_SECONDS = 600;
    private static final int MAX_ATTEMPTS = 5;

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
        store.put(email.toLowerCase(), new OtpEntry(code, Instant.now().plusSeconds(OTP_TTL_SECONDS), new AtomicInteger()));
        LOGGER.infof("OTP generated for %s", email);

        String htmlBody = """
        <!DOCTYPE html>
        <html>
        <body style="margin: 0; padding: 24px; background: #f7f7fb; font-family: Inter, Arial, sans-serif; color: #1f2937;">
            <div style="max-width: 520px; margin: 0 auto; background: #fff; border: 1px solid #ececf2; border-radius: 14px; padding: 28px;">
                <div style="font-size: 22px; font-weight: 700; color: #4f46e5; margin-bottom: 8px;">Mixpla</div>
                <h2 style="font-size: 20px; margin: 0 0 12px;">Your confirmation code</h2>
                <p style="margin: 0 0 18px; color: #4b5563; line-height: 1.45;">Use this code to confirm your upload. It stays active for 10 minutes.</p>
                <div style="margin: 16px 0 18px; background: #f3f4ff; border: 1px solid #dfe1ff; border-radius: 12px; text-align: center; padding: 18px;">
                    <div style="font-size: 34px; letter-spacing: 6px; font-weight: 700; color: #312e81; font-family: 'Courier New', monospace;">%s</div>
                </div>
                <p style="margin: 0; color: #6b7280; font-size: 14px; line-height: 1.45;">If this was not you, just ignore this email.</p>
            </div>
        </body>
        </html>
        """.formatted(code);

        String textBody = "Your confirmation code is: " + code + "\n\nIt expires in 10 minutes.";

        return mailer.send(Mail.withHtml(email, "Confirmation Code", htmlBody).setText(textBody));
    }

    /**
     * Non-consuming check, kept for the public song submission flow: the code is re-checked on every
     * chunk of a chunked upload, so it has to stay valid for the whole batch.
     */
    public boolean isVerifyFail(String email, String code) {
        return !verify(email, code, false);
    }

    /**
     * Consuming check for the login flow - a code that bought a token is spent immediately.
     */
    public boolean verifyAndConsume(String email, String code) {
        return verify(email, code, true);
    }

    private boolean verify(String email, String code, boolean consume) {
        if (isTestBypass(email, code)) {
            return true;
        }
        String key = email.toLowerCase();
        OtpEntry entry = store.get(key);
        if (entry == null || Instant.now().isAfter(entry.expiry())) {
            store.remove(key);
            return false;
        }
        if (entry.attempts().incrementAndGet() > MAX_ATTEMPTS) {
            store.remove(key);
            LOGGER.warnf("OTP attempt limit exceeded for %s, code invalidated", email);
            return false;
        }
        if (!entry.code().equals(code)) {
            return false;
        }
        if (consume) {
            store.remove(key);
        }
        return true;
    }

    /**
     * Never honoured in production, regardless of configuration - it would be a fixed-credential
     * backdoor into any account once a verified code yields a Keycloak token.
     */
    private boolean isTestBypass(String email, String code) {
        if (!LaunchMode.current().isDevOrTest()) {
            return false;
        }
        String bypassEmail = config.getOtpTestBypassEmail();
        String bypassCode = config.getOtpTestBypassCode();
        if (bypassEmail.isBlank() || bypassCode.isBlank()) {
            return false;
        }
        return bypassEmail.equalsIgnoreCase(email) && bypassCode.equals(code);
    }

    private String generateCode() {
        int n = rng.nextInt(1_000_000);
        return String.format("%06d", n);
    }

    private record OtpEntry(String code, Instant expiry, AtomicInteger attempts) {}
}
