package com.semantyca.datanest.config;

import io.quarkus.oidc.TenantResolver;
import io.vertx.ext.web.RoutingContext;
import jakarta.enterprise.context.ApplicationScoped;

/**
 * Splits authentication by path instead of by application-wide setting.
 *
 * The default tenant stays application-type=web-app for the 42next admin FE, which relies on the
 * authorization code flow. The Mixdeck routes resolve to a service (bearer-only) tenant against the
 * same realm, so tokens obtained through the OTP login are accepted from the Authorization header
 * and no redirect is ever issued.
 *
 * A single global application-type (including hybrid) cannot do this - it would change 42next too.
 */
@ApplicationScoped
public class MixdeckTenantResolver implements TenantResolver {

    static final String MIXDECK_TENANT = "mixdeck";

    private static final String[] MIXDECK_PATHS = {
            "/datanest/public/",
            "/datanest/dictionary/",
            "/datanest/soundfragments-bulk/"
    };

    @Override
    public String resolve(RoutingContext context) {
        String path = context.normalizedPath();
        for (String prefix : MIXDECK_PATHS) {
            if (path.startsWith(prefix)) {
                return MIXDECK_TENANT;
            }
        }
        return null;
    }
}
