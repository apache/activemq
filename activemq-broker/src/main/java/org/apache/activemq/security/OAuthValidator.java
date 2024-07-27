package org.apache.activemq.security;

public class OAuthValidator {
    private String clientId;
    private String clientSecret;
    private String oidcServerUrl;
    private String oidcIssuer;

    public OAuthValidator(String clientId, String clientSecret, String oidcServerUrl, String oidcIssuer) {
        this.clientId = clientId;
        this.clientSecret = clientSecret;
        this.oidcServerUrl = oidcServerUrl;
        this.oidcIssuer = oidcIssuer;
    }

    public void initialize() {
        throw new UnsupportedOperationException("Method not implemented yet");
    }

    public boolean validateToken(String token) {
        throw new UnsupportedOperationException("Method not implemented yet");
    }
}
