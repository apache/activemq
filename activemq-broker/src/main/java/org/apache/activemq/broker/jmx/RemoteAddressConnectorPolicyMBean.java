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
package org.apache.activemq.broker.jmx;

/**
 * Management view of a {@link org.apache.activemq.broker.RemoteAddressConnectorPolicy}:
 * the configured CIDR lists, the list load metrics and the admission counters.
 */
public interface RemoteAddressConnectorPolicyMBean extends TransportConnectorPolicyMBean {

    @MBeanInfo("Remote address allow list: comma separated CIDR blocks or a file: URI")
    String getAllowList();

    @MBeanInfo("Remote address deny list: comma separated CIDR blocks or a file: URI")
    String getDenyList();

    @MBeanInfo("Valid CIDR entries loaded into the allow list")
    long getAllowListCount();

    @MBeanInfo("Valid CIDR entries loaded into the deny list")
    long getDenyListCount();

    @MBeanInfo("Allow list entries skipped as invalid")
    long getAllowListInvalidCount();

    @MBeanInfo("Deny list entries skipped as invalid")
    long getDenyListInvalidCount();

    @MBeanInfo("Connections accepted by this policy since the last statistics reset")
    long getAllowedCount();

    @MBeanInfo("Connections refused by this policy since the last statistics reset")
    long getDeniedCount();

    @MBeanInfo("Reset the allowed and denied counters; list metrics are unaffected")
    void resetStatistics();

    @MBeanInfo("Would the IP address, or the CIDR block as a whole, be allowed by the lists (enabled flag ignored)")
    boolean allowed(@MBeanInfo("addressOrCidr") String addressOrCidr);

}
