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
