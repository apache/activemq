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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import javax.net.ssl.SSLParameters;

import org.junit.Test;

/**
 * Runs against the activemq-client jar, so the multi-release variant matching
 * the running JDK is the one under test: 17 to 20 refuse everything, 21 to 26
 * apply groups and schemes, 27 and later also honour the post-quantum switch.
 * The getters are read reflectively because this module compiles for Java 17.
 */
public class SslParameterSupportTest {

    private static final int JAVA = Runtime.version().feature();

    @Test
    public void availabilityFollowsTheRunningJdk() {
        assertEquals(JAVA >= 21, SslParameterSupport.isNamedGroupsAvailable());
        assertEquals(JAVA >= 27, SslParameterSupport.isPostQuantumKeyExchangeAvailable());
    }

    @Test
    public void nothingRequestedChangesNothing() {
        var parameters = new SSLParameters();
        assertFalse(SslParameterSupport.apply(parameters, null, null, false));
        assertFalse(SslParameterSupport.apply(parameters, new String[0], new String[0], false));
    }

    @Test
    public void requireAndNamedGroupsTogetherAreRejectedOnEveryJdk() {
        var thrown = assertThrows(IllegalArgumentException.class,
                () -> SslParameterSupport.apply(new SSLParameters(), new String[] {"x25519"}, null, true));
        assertTrue(thrown.getMessage(), thrown.getMessage().contains("cannot both be set"));
    }

    @Test
    public void namedGroupsAndSignatureSchemes() throws Exception {
        var parameters = new SSLParameters();
        String[] groups = {"x25519", "secp384r1"};
        String[] schemes = {"ed25519", "rsa_pss_rsae_sha256"};
        if (JAVA >= 21) {
            assertTrue(SslParameterSupport.apply(parameters, groups, schemes, false));
            assertArrayEquals(groups, read(parameters, "getNamedGroups"));
            assertArrayEquals(schemes, read(parameters, "getSignatureSchemes"));
        } else {
            var thrown = assertThrows(IllegalStateException.class, () -> SslParameterSupport.apply(parameters, groups, schemes, false));
            assertTrue(thrown.getMessage(), thrown.getMessage().contains("Java 21"));
        }
    }

    @Test
    public void requirePostQuantumKeyExchange() throws Exception {
        var parameters = new SSLParameters();
        if (JAVA >= 27) {
            assertTrue(SslParameterSupport.apply(parameters, null, null, true));
            assertArrayEquals(SslParameterSupport.POST_QUANTUM_NAMED_GROUPS, read(parameters, "getNamedGroups"));
        } else {
            var thrown = assertThrows(IllegalStateException.class, () -> SslParameterSupport.apply(parameters, null, null, true));
            assertTrue(thrown.getMessage(), thrown.getMessage().contains("Java 27"));
        }
    }

    @Test
    public void effectiveNamedGroupsDescribeWhatWillBeRequested() {
        assertNull(SslParameterSupport.effectiveNamedGroups(null, false));
        assertArrayEquals(new String[] {"x25519"}, SslParameterSupport.effectiveNamedGroups(new String[] {"x25519"}, false));
        assertArrayEquals(SslParameterSupport.POST_QUANTUM_NAMED_GROUPS, SslParameterSupport.effectiveNamedGroups(null, true));
        assertEquals("X25519MLKEM768", SslParameterSupport.POST_QUANTUM_NAMED_GROUPS[0]);
    }

    private static String[] read(SSLParameters parameters, String getter) throws Exception {
        return (String[]) SSLParameters.class.getMethod(getter).invoke(parameters);
    }
}
