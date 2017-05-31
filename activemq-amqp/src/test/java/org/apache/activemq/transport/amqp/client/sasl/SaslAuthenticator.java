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
package org.apache.activemq.transport.amqp.client.sasl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.jms.JMSSecurityException;
import javax.security.sasl.SaslException;

import org.apache.qpid.proton.engine.Sasl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manage the SASL authentication process
 */
public class SaslAuthenticator {

    private static final Logger LOG = LoggerFactory.getLogger(SaslAuthenticator.class);

    private final Sasl sasl;
    private final String username;
    private final String password;
    private final String authzid;
    private Mechanism mechanism;
    private String mechanismRestriction;

    /**
     * Create the authenticator and initialize it.
     *
     * @param sasl
     *        The Proton SASL entry point this class will use to manage the authentication.
     * @param username
     *        The user name that will be used to authenticate.
     * @param password
     *        The password that will be used to authenticate.
     * @param authzid
     *        The authzid used when authenticating (currently only with PLAIN)
     * @param mechanismRestriction
     *        A particular mechanism to use (if offered by the server) or null to allow selection.
     */
    public SaslAuthenticator(Sasl sasl, String username, String password, String authzid, String mechanismRestriction) {
        this.sasl = sasl;
        this.username = username;
        this.password = password;
        this.authzid = authzid;
        this.mechanismRestriction = mechanismRestriction;
    }

    /**
     * Process the SASL authentication cycle until such time as an outcome is determine. This
     * method must be called by the managing entity until the return value is true indicating a
     * successful authentication or a JMSSecurityException is thrown indicating that the
     * handshake failed.
     *
     * @throws JMSSecurityException
     */
    public boolean authenticate() throws SecurityException {
        switch (sasl.getState()) {
            case PN_SASL_IDLE:
                handleSaslInit();
                break;
            case PN_SASL_STEP:
                handleSaslStep();
                break;
            case PN_SASL_FAIL:
                handleSaslFail();
                break;
            case PN_SASL_PASS:
                return true;
            default:
        }

        return false;
    }

    private void handleSaslInit() throws SecurityException {
        try {
            String[] remoteMechanisms = sasl.getRemoteMechanisms();
            if (remoteMechanisms != null && remoteMechanisms.length != 0) {
                mechanism = findMatchingMechanism(remoteMechanisms);
                if (mechanism != null) {
                    mechanism.setUsername(username);
                    mechanism.setPassword(password);
                    mechanism.setAuthzid(authzid);
                    // TODO - set additional options from URI.
                    // TODO - set a host value.

                    sasl.setMechanisms(mechanism.getName());
                    byte[] response = mechanism.getInitialResponse();
                    if (response != null && response.length != 0) {
                        sasl.send(response, 0, response.length);
                    }
                } else {
                    // TODO - Better error message.
                    throw new SecurityException("Could not find a matching SASL mechanism for the remote peer.");
                }
            }
        } catch (SaslException se) {
            // TODO - Better error message.
            SecurityException jmsse = new SecurityException("Exception while processing SASL init.");
            jmsse.initCause(se);
            throw jmsse;
        }
    }

    private Mechanism findMatchingMechanism(String...remoteMechanisms) {

        Mechanism match = null;
        List<Mechanism> found = new ArrayList<Mechanism>();

        for (String remoteMechanism : remoteMechanisms) {
            if(mechanismRestriction != null && !mechanismRestriction.equals(remoteMechanism)) {
                LOG.debug("Skipping {} mechanism because it is not the configured mechanism restriction {}", remoteMechanism, mechanismRestriction);
                continue;
            }

            Mechanism mechanism = null;
            if (remoteMechanism.equalsIgnoreCase("PLAIN")) {
                mechanism = new PlainMechanism();
            } else if (remoteMechanism.equalsIgnoreCase("ANONYMOUS")) {
                mechanism = new AnonymousMechanism();
            } else if (remoteMechanism.equalsIgnoreCase("CRAM-MD5")) {
                mechanism = new CramMD5Mechanism();
            } else {
                LOG.debug("Unknown remote mechanism {}, skipping", remoteMechanism);
                continue;
            }

            if (mechanism.isApplicable(username, password)) {
                found.add(mechanism);
            }
        }

        if (!found.isEmpty()) {
            // Sorts by priority using Mechanism comparison and return the last value in
            // list which is the Mechanism deemed to be the highest priority match.
            Collections.sort(found);
            match = found.get(found.size() - 1);
        }

        LOG.info("Best match for SASL auth was: {}", match);

        return match;
    }

    private void handleSaslStep() throws SecurityException {
        try {
            if (sasl.pending() != 0) {
                byte[] challenge = new byte[sasl.pending()];
                sasl.recv(challenge, 0, challenge.length);
                byte[] response = mechanism.getChallengeResponse(challenge);
                sasl.send(response, 0, response.length);
            }
        } catch (SaslException se) {
            // TODO - Better error message.
            SecurityException jmsse = new SecurityException("Exception while processing SASL step.");
            jmsse.initCause(se);
            throw jmsse;
        }
    }

    private void handleSaslFail() throws SecurityException {
        // TODO - Better error message.
        throw new SecurityException("Client failed to authenticate");
    }
}
