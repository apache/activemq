/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.transport.amqp.sasl;

import java.security.Principal;
import java.security.cert.X509Certificate;
import java.util.Set;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.security.AuthenticationBroker;
import org.apache.activemq.security.SecurityContext;
import org.apache.activemq.transport.amqp.AmqpTransport;
import org.apache.qpid.proton.engine.Sasl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SASL Authentication engine.
 */
public class AmqpAuthenticator {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpAuthenticator.class);

    private static final String[] mechanisms = new String[] { "PLAIN", "ANONYMOUS" };

    private final BrokerService brokerService;
    private final AmqpTransport transport;
    private final Sasl sasl;

    private AuthenticationBroker authenticator;

    public AmqpAuthenticator(AmqpTransport transport, Sasl sasl, BrokerService brokerService) {
        this.brokerService = brokerService;
        this.transport = transport;
        this.sasl = sasl;

        sasl.setMechanisms(mechanisms);
        sasl.server();
    }

    /**
     * @return true if the SASL exchange has completed, regardless of success.
     */
    public boolean isDone() {
        return sasl.getOutcome() != Sasl.SaslOutcome.PN_SASL_NONE;
    }

    /**
     * @return the list of all SASL mechanisms that are supported currently.
     */
    public String[] getSupportedMechanisms() {
        return mechanisms;
    }

    public void processSaslExchange(ConnectionInfo connectionInfo) {
        if (sasl.getRemoteMechanisms().length > 0) {

            SaslMechanism mechanism = getSaslMechanism(sasl.getRemoteMechanisms());
            if (mechanism != null) {
                LOG.debug("SASL [{}} Handshake started.", mechanism.getMechanismName());

                mechanism.processSaslStep(sasl);
                if (!mechanism.isFailed()) {

                    connectionInfo.setUserName(mechanism.getUsername());
                    connectionInfo.setPassword(mechanism.getPassword());

                    if (tryAuthenticate(connectionInfo, transport.getPeerCertificates())) {
                        sasl.done(Sasl.SaslOutcome.PN_SASL_OK);
                    } else {
                        sasl.done(Sasl.SaslOutcome.PN_SASL_AUTH);
                    }

                    LOG.debug("SASL [{}} Handshake complete.", mechanism.getMechanismName());
                } else {
                    LOG.debug("SASL [{}} Handshake failed: {}", mechanism.getMechanismName(), mechanism.getFailureReason());
                    sasl.done(Sasl.SaslOutcome.PN_SASL_AUTH);
                }
            } else {
                LOG.info("SASL: could not find supported mechanism");
                sasl.done(Sasl.SaslOutcome.PN_SASL_PERM);
            }
        }
    }

    //----- Internal implementation ------------------------------------------//

    private SaslMechanism getSaslMechanism(String[] remoteMechanisms) {
        String primary = remoteMechanisms[0];

        if (primary.equalsIgnoreCase("PLAIN")) {
            return new PlainMechanism();
        } else if (primary.equalsIgnoreCase("ANONYMOUS")) {
            return new AnonymousMechanism();
        }

        return null;
    }

    private boolean tryAuthenticate(ConnectionInfo info, X509Certificate[] peerCertificates) {
        try {
            return getAuthenticator().authenticate(info.getUserName(), info.getPassword(), peerCertificates) != null;
        } catch (Throwable error) {
            return false;
        }
    }

    private AuthenticationBroker getAuthenticator() {
        if (authenticator == null) {
            try {
                authenticator = (AuthenticationBroker) brokerService.getBroker().getAdaptor(AuthenticationBroker.class);
            } catch (Exception e) {
                LOG.debug("Failed to lookup AuthenticationBroker from Broker, will use a default Noop version.");
            }

            if (authenticator == null) {
                authenticator = new DefaultAuthenticationBroker();
            }
        }

        return authenticator;
    }

    private class DefaultAuthenticationBroker implements AuthenticationBroker {

        @Override
        public SecurityContext authenticate(String username, String password, X509Certificate[] peerCertificates) throws SecurityException {
            return new SecurityContext(username) {

                @Override
                public Set<Principal> getPrincipals() {
                    return null;
                }
            };
        }
    }
}
