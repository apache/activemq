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

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class CidrConverterTest {

    @Test
    public void testHostNamesAreNotResolved() {
        // a CIDR must start with an IP literal; a host name is a configuration error, never a DNS lookup
        assertThrows(IllegalArgumentException.class, () -> CidrConverter.fromString("localhost/32"));
        assertThrows(IllegalArgumentException.class, () -> CidrConverter.fromString("example.com/24"));
        assertEquals(0, CidrConverter.parseCidrStrings(java.util.List.of("example.com/24"), "test").size());
        assertNotNull(CidrConverter.parseIpLiteral("192.168.1.1"));
        assertNotNull(CidrConverter.parseIpLiteral("2001:db8::1"));
        assertThrows(IllegalArgumentException.class, () -> CidrConverter.parseIpLiteral("localhost"));
    }

    @Test
    public void fromStringValid() {
        var cidr = CidrConverter.fromString("192.168.1.1/32");
        assertEquals("192.168.1.1/32", cidr.cidr());
        assertArrayEquals(new byte[]{-1, -1, -1, -1}, cidr.mask());
        assertArrayEquals(new byte[]{-64, -88, 1, 1}, cidr.networkAddress());

        cidr = CidrConverter.fromString("192.168.1.0/24");
        assertEquals("192.168.1.0/24", cidr.cidr());
        assertArrayEquals(new byte[]{-1, -1, -1, 0}, cidr.mask());
        assertArrayEquals(new byte[]{-64, -88, 1, 0}, cidr.networkAddress());

        cidr = CidrConverter.fromString("192.168.0.0/16");
        assertEquals("192.168.0.0/16", cidr.cidr());
        assertArrayEquals(new byte[]{-1, -1, 0, 0}, cidr.mask());
        assertArrayEquals(new byte[]{-64, -88, 0, 0}, cidr.networkAddress());

        cidr = CidrConverter.fromString("192.0.0.0/8");
        assertEquals("192.0.0.0/8", cidr.cidr());
        assertArrayEquals(new byte[]{-1, 0, 0, 0}, cidr.mask());
        assertArrayEquals(new byte[]{-64, 0, 0, 0}, cidr.networkAddress());

        cidr = CidrConverter.fromString("0.0.0.0/0");
        assertEquals("0.0.0.0/0", cidr.cidr());
        assertArrayEquals(new byte[]{0, 0, 0, 0}, cidr.mask());
        assertArrayEquals(new byte[]{0, 0, 0, 0}, cidr.networkAddress());

        cidr = CidrConverter.fromString("127.0.0.1/32");
        assertEquals("127.0.0.1/32", cidr.cidr());
        assertArrayEquals(new byte[]{-1, -1, -1, -1}, cidr.mask());
        assertArrayEquals(new byte[]{127, 0, 0, 1}, cidr.networkAddress());
    }

    @Test
    public void fromStringInvalid() {
        var invalidInputs = new String[]{"not-an-ip/16", "1.2.3.4/-32", "1.2.3.4/33", "1.2.3.4/a", "1.2.3.4", "invalid3", "", "256.3.5.2/32"};

        for (var input : invalidInputs) {
            assertThrows(IllegalArgumentException.class, () -> {
                CidrConverter.fromString(input);
            });
        }
    }

    @Test
    public void parseCidrStrings() {
        var cidrs = CidrConverter.parseCidrStrings(null, "empty.txt");
        assertNotNull(cidrs);
        assertTrue(cidrs.isEmpty());

        cidrs = CidrConverter.parseCidrStrings(List.of(), "empty.txt");
        assertNotNull(cidrs);
        assertTrue(cidrs.isEmpty());

        cidrs = CidrConverter.parseCidrStrings(List.of(""), "empty.txt");
        assertNotNull(cidrs);
        assertTrue(cidrs.isEmpty());

        cidrs = CidrConverter.parseCidrStrings(List.of("192.168.1.1/32", "192.168.2.0/24", "10.10.0.0/16", "11.0.0.0/8"), "four.txt");
        assertNotNull(cidrs);
        assertFalse(cidrs.isEmpty());
        assertEquals(Integer.valueOf(4), Integer.valueOf(cidrs.size()));

        cidrs = CidrConverter.parseCidrStrings(List.of("192.168.1.1/32", "192.168.2.0/24", "10.10.0.0/16", "1.2.3.4/-32"), "four-one-invalid.txt");
        assertNotNull(cidrs);
        assertFalse(cidrs.isEmpty());
        assertEquals(Integer.valueOf(3), Integer.valueOf(cidrs.size()));
    }
//            {"192.168.1.50",        "ALLOW  — in allow /24, not denied"},
//            {"192.168.1.100",       "DENY   — explicit deny /32"},
//            {"10.5.6.7",            "ALLOW  — in allow 10/8, not denied"},
//            {"10.0.0.1",            "DENY   — explicit deny /32"},
//            {"10.255.255.99",       "DENY   — in denied /24 subnet"},
//            {"172.16.5.1",          "ALLOW  — in allow 172.16/12"},
//            {"8.8.8.8",             "DENY   — not in any allow entry"},
//            // ---- IPv6 ----
//            {"::1",                 "ALLOW  — loopback in allow list"},
//            {"2001:db8::1",         "ALLOW  — in allow 2001:db8::/32"},
//            {"2001:db8:bad::1",     "DENY   — in deny 2001:db8:bad::/48"},
//            {"fe80::1",             "ALLOW  — link-local in allow fe80::/10"},
//            {"2001:db9::1",         "DENY   — not in any allow entry"},
//            // ---- IPv4-mapped IPv6 (dual-stack sockets) ----
//            {"::ffff:192.168.1.50", "ALLOW  — mapped; normalised to 192.168.1.50"},
//            {"::ffff:192.168.1.100","DENY   — mapped; normalised, hits deny /32"},
//            {"::ffff:8.8.8.8",      "DENY   — mapped; normalised, not in allow"},
//    };

}
