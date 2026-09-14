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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CidrConverter {

    private static final Logger logger = LoggerFactory.getLogger(CidrConverter.class);

    public static List<Cidr> parseCidrStrings(final List<String> cidrStrings, final String locationHint) {
        if (cidrStrings == null || cidrStrings.isEmpty()) {
            return Collections.emptyList();
        }

        var cidrs = new ArrayList<Cidr>(cidrStrings.size());
        for (var cidrString : cidrStrings) {
            var trimmedCidrString = cidrString.trim();

            if (trimmedCidrString.isBlank()) {
                continue;
            }

            try {
                cidrs.add(CidrConverter.fromString(trimmedCidrString));
            } catch (IllegalArgumentException e) {
                logger.warn("Invalid CIDR string:{} from:{}", trimmedCidrString, locationHint);
            }
        }
        return Collections.unmodifiableList(cidrs);
    }

    public static Cidr fromString(String cidr) {
        int slash = cidr.lastIndexOf('/');
        if (slash < 0) {
            throw new IllegalArgumentException("Invalid CIDR (missing '/'): " + cidr);
        }

        String ipPart = cidr.substring(0, slash);
        String prefixPart = cidr.substring(slash + 1);

        InetAddress base;
        try {
            base = parseIpLiteral(ipPart);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid IP in CIDR: " + cidr, e);
        }

        var mask = parseMask(cidr, base, prefixPart);
        var networkAddress = Cidr.applyMask(base.getAddress(), mask);
        return new Cidr(cidr, networkAddress, mask);
    }

    /**
     * Parses an IPv4 or IPv6 literal. Host names are rejected rather than resolved,
     * so configuration and JMX input never trigger a DNS lookup.
     *
     * @throws IllegalArgumentException if the value is not an IP literal
     */
    public static InetAddress parseIpLiteral(String value) {
        var trimmed = value == null ? "" : value.trim();
        var looksLikeIpv4 = trimmed.matches("\\d{1,3}(\\.\\d{1,3}){3}");
        var looksLikeIpv6 = trimmed.indexOf(':') >= 0 && trimmed.matches("[0-9a-fA-F:.]+(%[0-9a-zA-Z]+)?");
        if (!looksLikeIpv4 && !looksLikeIpv6) {
            throw new IllegalArgumentException("Not an IP address literal: " + value);
        }
        try {
            // a literal never triggers a lookup
            return InetAddress.getByName(trimmed);
        } catch (UnknownHostException e) {
            throw new IllegalArgumentException("Not an IP address literal: " + value, e);
        }
    }

    private static byte[] parseMask(String cidr, InetAddress base, String prefixPart) {
        int totalBits = base.getAddress().length * 8; // 32 or 128
        int prefix;
        try {
            prefix = Integer.parseInt(prefixPart);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid prefix length in CIDR: " + cidr, e);
        }
        if (prefix < 0 || prefix > totalBits) {
            throw new IllegalArgumentException("Prefix length out of range [0," + totalBits + "]: " + cidr);
        }

        var mask = Cidr.buildMask(totalBits / 8, prefix);
        return mask;
    }
}
