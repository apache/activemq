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
package org.apache.activemq.util;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.Socket;
import java.util.List;

import org.junit.Test;

public class RemoteAddressValidatorTest {

    private static List<Cidr> cidrs(String... blocks) {
        return CidrListLoader.parse(List.of(blocks), "test").cidrs();
    }

    private static InetAddress ip(String address) throws Exception {
        return InetAddress.getByName(address);
    }

    @Test
    public void testDenyIsCheckedBeforeAllow() throws Exception {
        var validator = new RemoteAddressValidator(cidrs("10.0.0.0/8"), cidrs("10.1.0.0/16"));
        assertFalse("inside the deny block", validator.isAllowed(ip("10.1.2.3")));
        assertTrue("inside allow, outside deny", validator.isAllowed(ip("10.2.0.1")));
    }

    @Test
    public void testEmptyAllowListPermitsEverythingNotDenied() throws Exception {
        var validator = new RemoteAddressValidator(cidrs(), cidrs("192.168.0.0/16"));
        assertTrue(validator.isAllowed(ip("8.8.8.8")));
        assertFalse(validator.isAllowed(ip("192.168.1.1")));
    }

    @Test
    public void testAddressOutsideAllowListIsDenied() throws Exception {
        var validator = new RemoteAddressValidator(cidrs("10.0.0.0/8"), cidrs());
        assertTrue(validator.isAllowed(ip("10.255.255.254")));
        assertFalse(validator.isAllowed(ip("172.16.0.1")));
    }

    @Test
    public void testIpv4MappedIpv6MatchesIpv4Rule() throws Exception {
        var validator = new RemoteAddressValidator(cidrs("192.168.1.0/24"), cidrs());
        // build the mapped form explicitly: InetAddress.getByName would already collapse it to IPv4
        var mapped = new byte[16];
        mapped[10] = (byte) 0xFF;
        mapped[11] = (byte) 0xFF;
        mapped[12] = (byte) 192;
        mapped[13] = (byte) 168;
        mapped[14] = 1;
        mapped[15] = 5;
        var address = Inet6Address.getByAddress(null, mapped, -1);
        assertTrue("::ffff:192.168.1.5 must match 192.168.1.0/24", validator.isAllowed(address));
    }

    @Test
    public void testIpv6RuleDoesNotMatchIpv4Address() throws Exception {
        var validator = new RemoteAddressValidator(cidrs("2001:db8::/32"), cidrs());
        assertTrue(validator.isAllowed(ip("2001:db8::1")));
        assertFalse(validator.isAllowed(ip("10.0.0.1")));
    }

    @Test
    public void testSocketWithoutRemoteAddressIsDenied() throws Exception {
        var validator = new RemoteAddressValidator(cidrs(), cidrs());
        assertFalse(validator.isAllowed((Socket) null));
        try (var unconnected = new Socket()) {
            assertFalse(validator.isAllowed(unconnected));
        }
    }

    @Test
    public void testStringQueryRunsFullDecisionForSingleAddress() {
        var validator = new RemoteAddressValidator(cidrs("10.0.0.0/8", "2001:db8::/32"), cidrs("10.1.0.0/16"));
        assertTrue(validator.isAllowed("10.2.3.4"));
        assertTrue(validator.isAllowed(" 2001:db8::1 "));
        assertFalse("deny wins over the allow entry that also matches", validator.isAllowed("10.1.2.3"));
        assertFalse("outside the allow list", validator.isAllowed("172.16.0.1"));
        assertTrue("mapped form is normalised before the decision", validator.isAllowed("::ffff:10.2.3.4"));
    }

    @Test
    public void testStringQueryIgnoresEnabledFlag() {
        var validator = new RemoteAddressValidator(cidrs(), cidrs("10.0.0.0/8"));
        validator.setEnabled(false);
        assertFalse(validator.isAllowed("10.0.0.1"));
        assertTrue(validator.isAllowed("192.168.0.1"));
    }

    @Test
    public void testBlockJudgedAsAWholeDespiteCarveOuts() {
        // a class B is allowed while its core router and DNS server are denied
        var validator = new RemoteAddressValidator(cidrs("10.20.0.0/16"), cidrs("10.20.0.1/32", "10.20.0.53/32"));
        assertTrue("the network is allowed even though two hosts inside it are refused", validator.isAllowed("10.20.0.0/16"));
        assertTrue("a sub block clear of the carve outs", validator.isAllowed("10.20.5.0/24"));
        assertFalse("the denied host itself", validator.isAllowed("10.20.0.53"));
        assertFalse("a block entirely inside a deny entry", validator.isAllowed("10.20.0.53/32"));
        assertTrue("a sub block that contains a denied host is still allowed as a whole", validator.isAllowed("10.20.0.0/24"));
        assertFalse("wider than any allow entry", validator.isAllowed("10.0.0.0/8"));
        var noAllowList = new RemoteAddressValidator(cidrs(), cidrs("192.168.0.0/16"));
        assertTrue("empty allow list permits any block not wholly denied", noAllowList.isAllowed("192.0.0.0/8"));
        assertFalse(noAllowList.isAllowed("192.168.4.0/24"));
    }

    @Test
    public void testStringQueryRejectsNonLiterals() {
        var validator = new RemoteAddressValidator(cidrs("10.0.0.0/8"), cidrs());
        assertThrows(IllegalArgumentException.class, () -> validator.isAllowed("localhost"));
        assertThrows(IllegalArgumentException.class, () -> validator.isAllowed("example.com/24"));
        assertThrows(IllegalArgumentException.class, () -> validator.isAllowed(""));
        assertThrows(IllegalArgumentException.class, () -> validator.isAllowed((String) null));
    }

    @Test
    public void testEnabledToggle() {
        var validator = new RemoteAddressValidator(cidrs(), cidrs());
        assertTrue(validator.isEnabled());
        validator.setEnabled(false);
        assertFalse(validator.isEnabled());
    }
}
