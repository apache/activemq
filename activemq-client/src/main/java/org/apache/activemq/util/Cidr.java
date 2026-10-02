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

import java.net.InetAddress;
import java.util.Arrays;
import java.util.Objects;

/**
 * An IPv4 or IPv6 network block in CIDR notation.
 *
 * <p>Deliberately a plain class rather than a record: the activemq-spring schema
 * generator parses this module's sources with QDox 1.x, which does not understand
 * record declarations and fails the activemq-spring build on one.
 */
public final class Cidr {

    private final String cidr;
    private final byte[] networkAddress;
    private final byte[] mask;

    public Cidr(String cidr, byte[] networkAddress, byte[] mask) {
        this.cidr = cidr;
        this.networkAddress = networkAddress;
        this.mask = mask;
    }

    /** the block as written, e.g. {@code 192.168.1.0/24} */
    public String cidr() {
        return cidr;
    }

    /** the network address with the mask applied */
    public byte[] networkAddress() {
        return networkAddress;
    }

    public byte[] mask() {
        return mask;
    }

    /** number of leading one bits in the mask, i.e. the prefix length after the slash */
    public int prefixLength() {
        var bits = 0;
        for (var b : mask) {
            bits += Integer.bitCount(b & 0xFF);
        }
        return bits;
    }

    /** Returns {@code true} if every address of {@code other} falls within this block. */
    boolean covers(final Cidr other) {
        if (other.networkAddress.length != networkAddress.length || other.prefixLength() < prefixLength()) {
            return false;
        }
        return Arrays.equals(applyMask(other.networkAddress, mask), networkAddress);
    }

    /** Returns {@code true} if {@code address} falls within this block. */
    boolean contains(final InetAddress address) {
        var raw = address.getAddress();
        if (raw.length != networkAddress.length) {
            // IPv4 vs IPv6 mismatch — never match.
            return false;
        }
        var masked = applyMask(raw, mask);
        return Arrays.equals(masked, networkAddress);
    }

    static byte[] buildMask(final int byteLen, final int prefixBits) {
        var m = new byte[byteLen];
        for (var i = 0; i < byteLen; i++) {
            var bitsLeft = prefixBits - i * 8;
            if (bitsLeft >= 8) {
                m[i] = (byte) 0xFF;
            } else if (bitsLeft > 0) {
                m[i] = (byte) (0xFF & (0xFF << (8 - bitsLeft)));
            } else {
                m[i] = 0;
            }
        }
        return m;
    }

    static byte[] applyMask(final byte[] addr, final byte[] mask) {
        var result = new byte[addr.length];
        for (var i = 0; i < addr.length; i++) {
            result[i] = (byte) (addr[i] & mask[i]);
        }
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Cidr)) {
            return false;
        }
        var that = (Cidr) o;
        return Objects.equals(cidr, that.cidr)
                && Arrays.equals(networkAddress, that.networkAddress)
                && Arrays.equals(mask, that.mask);
    }

    @Override
    public int hashCode() {
        return 31 * (31 * Objects.hashCode(cidr) + Arrays.hashCode(networkAddress)) + Arrays.hashCode(mask);
    }

    @Override
    public String toString() {
        return cidr;
    }
}
