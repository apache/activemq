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
package org.apache.activemq.transport.tcp;

import javax.net.ssl.SSLParameters;

/**
 * Applies the TLS key exchange and signature settings of the ssl transports
 * ({@code namedGroups}, {@code signatureSchemes},
 * {@code requirePostQuantumKeyExchange}) to an {@link SSLParameters}.
 *
 * <p>This instance of the class is for JDK [21, 27): named groups and signature
 * schemes are applied, but the post-quantum hybrid groups of JEP 527 do not
 * exist before JDK 27, so requiring them fails fast rather than failing every
 * handshake later with an unhelpful alert.
 */
public final class SslParameterSupport {

    /** the hybrid key exchange groups of JEP 527, in preference order */
    public static final String[] POST_QUANTUM_NAMED_GROUPS = {"X25519MLKEM768", "SecP256r1MLKEM768", "SecP384r1MLKEM1024"};

    private SslParameterSupport() {
    }

    /** whether this JDK can restrict key exchange to the post-quantum hybrid groups */
    public static boolean isPostQuantumKeyExchangeAvailable() {
        return false;
    }

    /** whether this JDK can apply named groups and signature schemes at all */
    public static boolean isNamedGroupsAvailable() {
        return true;
    }

    /**
     * Validates the combination and sets the requested groups and schemes on the parameters.
     *
     * @return true when the parameters were changed
     * @throws IllegalArgumentException when requirePostQuantumKeyExchange and namedGroups are both set
     * @throws IllegalStateException when this JDK cannot honour a request
     */
    public static boolean apply(SSLParameters parameters, String[] namedGroups, String[] signatureSchemes, boolean requirePostQuantumKeyExchange) {
        checkCombination(namedGroups, requirePostQuantumKeyExchange);
        if (requirePostQuantumKeyExchange) {
            throw new IllegalStateException("requirePostQuantumKeyExchange requires Java 27 or later");
        }
        boolean changed = false;
        if (isSet(namedGroups)) {
            parameters.setNamedGroups(namedGroups.clone());
            changed = true;
        }
        if (isSet(signatureSchemes)) {
            parameters.setSignatureSchemes(signatureSchemes.clone());
            changed = true;
        }
        return changed;
    }

    /** the groups a connection will request: the explicit list, the post-quantum list, or null for the JDK default */
    public static String[] effectiveNamedGroups(String[] namedGroups, boolean requirePostQuantumKeyExchange) {
        if (requirePostQuantumKeyExchange) {
            return POST_QUANTUM_NAMED_GROUPS.clone();
        }
        return isSet(namedGroups) ? namedGroups.clone() : null;
    }

    static void checkCombination(String[] namedGroups, boolean requirePostQuantumKeyExchange) {
        if (requirePostQuantumKeyExchange && isSet(namedGroups)) {
            throw new IllegalArgumentException("requirePostQuantumKeyExchange and namedGroups cannot both be set");
        }
    }

    static boolean isSet(String[] values) {
        return values != null && values.length > 0;
    }
}
