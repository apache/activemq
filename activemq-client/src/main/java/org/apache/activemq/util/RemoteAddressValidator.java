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

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Validates a socket's remote IP address against configurable allow and deny
 * lists expressed in CIDR notation (e.g. "192.168.1.0/24", "10.0.0.0/8").
 *
 * <p>Evaluation order:
 * <ol>
 *   <li>If the address matches any <b>deny</b> entry → DENIED.</li>
 *   <li>If the allow list is non-empty and the address matches any <b>allow</b>
 *       entry → ALLOWED.</li>
 *   <li>If the allow list is empty (no restrictions) → ALLOWED.</li>
 *   <li>Otherwise → DENIED (not in allow list).</li>
 * </ol>
 *
 * <p>Both IPv4 and IPv6 addresses are supported. IPv4-mapped IPv6 addresses
 * (e.g. {@code ::ffff:192.168.1.1}) are automatically normalized to their
 * IPv4 equivalent before matching, so a single IPv4 CIDR rule covers both
 * plain IPv4 connections and dual-stack sockets that represent the same peer.
 *
 * <p>Build the lists with {@link CidrListLoader}, which accepts a comma separated
 * string or a {@code file:} URI and reports entries it had to skip.
 */
public class RemoteAddressValidator {

    private final List<Cidr> allowList;
    private final List<Cidr> denyList;
    private final AtomicBoolean enabled = new AtomicBoolean(true);

    /**
     * @param allowCidrs CIDR strings that are explicitly permitted.
     *                   Pass an empty list to permit all addresses.
     * @param denyCidrs  CIDR strings that are explicitly forbidden.
     *                   Deny rules are evaluated before allow rules.
     */
    public RemoteAddressValidator(final List<Cidr> allowCidrs, final List<Cidr> denyCidrs) {
        this.allowList = Collections.unmodifiableList(allowCidrs);
        this.denyList = Collections.unmodifiableList(denyCidrs);
    }

    public boolean isEnabled() {
        return enabled.get();
    }

    public void setEnabled(boolean enabled) {
        this.enabled.set(enabled);
    }

    /**
     * Returns {@code true} if the remote address of the given socket is allowed.
     *
     * @throws IllegalArgumentException if the socket is not connected or has
     *                                  no remote address.
     */
    public boolean isAllowed(Socket socket) {
        if (socket == null) {
            return false;
        }
        var remote = socket.getRemoteSocketAddress();
        if (!(remote instanceof InetSocketAddress)) {
            return false;
        }
        return isAllowed(((InetSocketAddress) remote).getAddress());
    }

    /**
     * Returns {@code true} if the given {@link InetAddress} passes the
     * allow/deny policy.
     */
    public boolean isAllowed(InetAddress address) {
        if (address == null) {
            return false;
        }

        // Normalise IPv4-mapped IPv6 addresses (::ffff:a.b.c.d) to plain IPv4
        // so that a dual-stack socket still matches IPv4 CIDR rules.
        address = normalise(address);

        // 1. Deny list takes priority.
        for (var block : denyList) {
            if (block.contains(address)) {
                return false;
            }
        }

        // 2. Allow list (empty = allow everything not denied).
        if (allowList.isEmpty()) {
            return true;
        }

        for (var block : allowList) {
            if (block.contains(address)) {
                return true;
            }
        }

        // 3. Not in any allow entry.
        return false;
    }

    /**
     * Runs a single IP address or a whole CIDR block through the allow/deny
     * decision. A single address gets exactly the decision an accepted socket
     * would: deny entries first, then the allow list, an empty allow list
     * permitting, an IPv4-mapped IPv6 address normalised first. A block is judged
     * as a whole: it is allowed when an allow entry covers it (or the allow list is
     * empty) and no deny entry covers the entire block, so a few denied hosts
     * carved out of an allowed network do not change its answer. The enabled flag
     * is ignored so lists can be checked before enforcement is switched on. Host
     * names are rejected, never resolved.
     *
     * @throws IllegalArgumentException if the value is neither an IP literal nor a CIDR block
     */
    public boolean isAllowed(String addressOrCidr) {
        if (addressOrCidr == null || addressOrCidr.isBlank()) {
            throw new IllegalArgumentException("an IP address or CIDR block is required");
        }
        var value = addressOrCidr.trim();
        if (value.indexOf('/') < 0) {
            return isAllowed(CidrConverter.parseIpLiteral(value));
        }
        var block = CidrConverter.fromString(value);
        for (var entry : denyList) {
            if (entry.covers(block)) {
                return false;
            }
        }
        if (allowList.isEmpty()) {
            return true;
        }
        for (var entry : allowList) {
            if (entry.covers(block)) {
                return true;
            }
        }
        return false;
    }

    /**
     * If {@code addr} is an IPv4-mapped IPv6 address ({@code ::ffff:a.b.c.d}),
     * returns the equivalent {@link Inet4Address}; otherwise returns {@code addr}
     * unchanged.
     *
     * <p>The JDK does not expose a public API for this, so we detect the
     * 16-byte pattern {@code [0,0,0,0, 0,0,0,0, 0,0,0xFF,0xFF, a,b,c,d]}
     * manually and reconstruct the IPv4 address from the last four bytes.
     */
    static InetAddress normalise(final InetAddress addr) {
        if (!(addr instanceof Inet6Address)) {
            return addr;
        }
        var raw = addr.getAddress(); // always 16 bytes for Inet6Address
        // Check for the ::ffff:0:0/96 prefix (bytes 0-9 = 0x00, bytes 10-11 = 0xFF)
        for (var i = 0; i < 10; i++) {
            if (raw[i] != 0) return addr;
        }
        if ((raw[10] & 0xFF) != 0xFF || (raw[11] & 0xFF) != 0xFF) {
            return addr;
        }
        // Extract the embedded IPv4 address from the last 4 bytes.
        var ipv4 = new byte[]{raw[12], raw[13], raw[14], raw[15]};
        try {
            return InetAddress.getByAddress(ipv4);
        } catch (UnknownHostException e) {
            // Should never happen for a 4-byte array; return original if it does.
            return addr;
        }
    }

}
