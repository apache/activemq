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
package org.apache.activemq.broker;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.Test;

/**
 * Unit coverage of the policy carrier on its own: eager list parsing, the
 * enabled gate, the admission counters and the dry run operation. Enforcement
 * through a connector is covered by TransportConnectorPolicyBrokerTest.
 */
public class RemoteAddressConnectorPolicyTest {

    private static final String LOOPBACK = "127.0.0.1/32";

    @Test
    public void testDefaults() {
        var policy = new RemoteAddressConnectorPolicy();
        assertEquals("remoteAddress", policy.getName());
        assertTrue(policy.isEnabled());
        assertEquals(0, policy.getAllowListCount());
        assertEquals(0, policy.getDenyListCount());
        assertTrue("no lists means every address is allowed", policy.allowed("192.0.2.1"));
    }

    @Test
    public void testListsParseWhenSet() {
        var policy = new RemoteAddressConnectorPolicy();
        policy.setAllowList("10.0.0.0/8,not-a-cidr," + LOOPBACK);
        policy.setDenyList("10.0.5.0/24");
        assertEquals(2, policy.getAllowListCount());
        assertEquals(1, policy.getAllowListInvalidCount());
        assertEquals(1, policy.getDenyListCount());
        assertEquals(0, policy.getDenyListInvalidCount());
        assertEquals("10.0.5.0/24", policy.getDenyList());
    }

    @Test
    public void testBrokenFileReferenceFailsAtConfigurationTime() {
        var policy = new RemoteAddressConnectorPolicy();
        assertThrows(IllegalArgumentException.class, () -> policy.setDenyList("file:/does/not/exist.txt"));
    }

    @Test
    public void testListLoadedFromFile() throws Exception {
        var dir = Files.createDirectories(Path.of("target", "remote-address-server-policy-test"));
        var file = Files.writeString(dir.resolve("deny.txt"), "# comment\n" + LOOPBACK + "\n", StandardCharsets.UTF_8);
        var policy = new RemoteAddressConnectorPolicy();
        policy.setDenyList("file:" + file.toAbsolutePath());
        assertEquals(1, policy.getDenyListCount());
        assertFalse(policy.allowed("127.0.0.1"));
    }

    @Test
    public void testProcessAdmitsAndCounts() throws Exception {
        var policy = new RemoteAddressConnectorPolicy();
        policy.setAllowList(LOOPBACK);
        withLoopbackSocket(socket -> {
            policy.process(socket);
            assertEquals(1, policy.getAllowedCount());
            assertEquals(0, policy.getDeniedCount());
        });
    }

    @Test
    public void testProcessRefusesAndCounts() throws Exception {
        var policy = new RemoteAddressConnectorPolicy();
        policy.setDenyList(LOOPBACK);
        withLoopbackSocket(socket -> {
            assertThrows(SecurityException.class, () -> policy.process(socket));
            assertEquals(0, policy.getAllowedCount());
            assertEquals(1, policy.getDeniedCount());
        });
    }

    @Test
    public void testDisabledPolicySkipsTheCheck() throws Exception {
        var policy = new RemoteAddressConnectorPolicy();
        policy.setDenyList(LOOPBACK);
        policy.setEnabled(false);
        withLoopbackSocket(socket -> {
            policy.process(socket);
            assertEquals(0, policy.getAllowedCount());
            assertEquals(0, policy.getDeniedCount());
        });
    }

    /** allowed() evaluates the lists even while enforcement is off, and judges a block as a whole. */
    @Test
    public void testAllowedDryRunIgnoresEnabledFlag() {
        var policy = new RemoteAddressConnectorPolicy();
        policy.setAllowList("10.20.0.0/16," + LOOPBACK);
        policy.setDenyList("10.20.0.53/32");
        policy.setEnabled(false);
        assertTrue(policy.allowed("127.0.0.1"));
        assertTrue(policy.allowed("10.20.7.8"));
        assertTrue(policy.allowed("10.20.0.0/16"));
        assertFalse("deny entry wins", policy.allowed("10.20.0.53"));
        assertFalse("outside the allow list", policy.allowed("192.0.2.1"));
        assertThrows(IllegalArgumentException.class, () -> policy.allowed("not-an-address"));
        assertEquals("dry runs are not counted", 0, policy.getAllowedCount());
        assertEquals(0, policy.getDeniedCount());
    }

    @Test
    public void testResetStatisticsClearsCountersNotListMetrics() throws Exception {
        var policy = new RemoteAddressConnectorPolicy();
        policy.setAllowList(LOOPBACK);
        withLoopbackSocket(policy::process);
        assertEquals(1, policy.getAllowedCount());
        policy.resetStatistics();
        assertEquals(0, policy.getAllowedCount());
        assertEquals(0, policy.getDeniedCount());
        assertEquals(1, policy.getAllowListCount());
    }

    private interface SocketCheck {
        void check(Socket socket) throws Exception;
    }

    /** Runs the check against the server side of a loopback connection, so the remote address is 127.0.0.1. */
    private static void withLoopbackSocket(SocketCheck check) throws Exception {
        try (var server = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
             var client = new Socket(InetAddress.getLoopbackAddress(), server.getLocalPort());
             var accepted = server.accept()) {
            check.check(accepted);
        }
    }
}
