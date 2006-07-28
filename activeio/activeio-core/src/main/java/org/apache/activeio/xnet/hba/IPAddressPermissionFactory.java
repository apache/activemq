/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activeio.xnet.hba;

/**
 * @version $Revision$ $Date$
 */
public class IPAddressPermissionFactory {

    public static IPAddressPermission getIPAddressMask(String mask) {
        if (StartWithIPAddressPermission.canSupport(mask)) {
            return new StartWithIPAddressPermission(mask);
        } else if (ExactIPAddressPermission.canSupport(mask)) {
            return new ExactIPAddressPermission(mask);
        } else if (FactorizedIPAddressPermission.canSupport(mask)) {
            return new FactorizedIPAddressPermission(mask);
        } else if (NetmaskIPAddressPermission.canSupport(mask)) {
            return new NetmaskIPAddressPermission(mask);
        } else if (ExactIPv6AddressPermission.canSupport(mask)) {
            return new ExactIPv6AddressPermission(mask);
        } else if (NetmaskIPv6AddressPermission.canSupport(mask)) {
            return new NetmaskIPv6AddressPermission(mask);
        }
        throw new IllegalArgumentException("Mask " + mask + " is not supported.");
    }
}
