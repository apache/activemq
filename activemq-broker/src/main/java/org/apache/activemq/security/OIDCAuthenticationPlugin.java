package org.apache.activemq.security;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerPluginSupport;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.security.OIDCSecurityContext;

public class OIDCAuthenticationPlugin implements BrokerPlugin {
    private String clientId;
    private String clientSecret;
    private String oidcServerUrl;
    private String oidcIssuer;

    @Override
    public Broker installPlugin(Broker broker) {
        return new OIDCBroker(broker);
    }

    private class OIDCBroker extends BrokerPluginSupport {
        private final Broker next;

        public OIDCBroker(Broker next) {
            this.next = next;
        }

        @Override
        public void addConnection(org.apache.activemq.broker.ConnectionContext context, ConnectionInfo info) throws Exception {
            throw new UnsupportedOperationException("Method not implemented yet");
        }

        private OIDCSecurityContext authenticate(String token) {
            throw new UnsupportedOperationException("Method not implemented yet");
        }
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public String getClientSecret() {
        return clientSecret;
    }

    public void setClientSecret(String clientSecret) {
        this.clientSecret = clientSecret;
    }

    public String getOidcServerUrl() {
        return oidcServerUrl;
    }

    public void setOidcServerUrl(String oidcServerUrl) {
        this.oidcServerUrl = oidcServerUrl;
    }

    public String getOidcIssuer() {
        return oidcIssuer;
    }

    public void setOidcIssuer(String oidcIssuer) {
        this.oidcIssuer = oidcIssuer;
    }
}