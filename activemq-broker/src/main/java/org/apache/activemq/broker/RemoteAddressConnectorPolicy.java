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

import java.net.Socket;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.activemq.broker.jmx.RemoteAddressConnectorPolicyMBean;
import org.apache.activemq.transport.TransportConnectorPolicy;
import org.apache.activemq.util.CidrListLoader;
import org.apache.activemq.util.RemoteAddressValidator;

/**
 * A {@link TransportConnectorPolicy} that admits or refuses each accepted socket
 * by its remote IP address, using allow and deny lists in CIDR notation.
 *
 * <p>Deny entries are checked first, then the allow list; an empty allow list
 * permits every address not denied. Each list is a comma separated string of
 * CIDR blocks or a {@code file:} URI to a file with one block per line; see
 * {@link CidrListLoader} for the accepted forms. A list is parsed when its
 * property is set, so a broken file reference fails the broker configuration
 * instead of the first connection.
 *
 * <p>The lists are configuration, set before the connector starts. At runtime
 * JMX exposes this policy as a {@link RemoteAddressConnectorPolicyMBean} under the
 * connector's object name: enforcement can be switched with the enabled
 * attribute, the counters report admissions and refusals, and the allowed
 * operation runs an address or block through the decision without connecting.
 *
 * <pre>
 * &lt;transportConnector name="openwire" uri="tcp://0.0.0.0:61616"&gt;
 *   &lt;transportConnectorPolicy&gt;
 *     &lt;remoteAddressConnectorPolicy allowList="10.0.0.0/8" denyList="10.0.0.53/32"/&gt;
 *   &lt;/transportConnectorPolicy&gt;
 * &lt;/transportConnector&gt;
 * </pre>
 *
 * @org.apache.xbean.XBean
 */
public class RemoteAddressConnectorPolicy implements TransportConnectorPolicy, RemoteAddressConnectorPolicyMBean {

    private String name = "remoteAddress";
    private volatile boolean enabled = true;
    private String allowList;
    private String denyList;

    private volatile RemoteAddressValidator validator;
    private volatile long allowListCount;
    private volatile long denyListCount;
    private volatile long allowListInvalidCount;
    private volatile long denyListInvalidCount;
    private final AtomicLong allowedCount = new AtomicLong();
    private final AtomicLong deniedCount = new AtomicLong();

    public RemoteAddressConnectorPolicy() {
        rebuild();
    }

    @Override
    public void process(final Socket socket) throws SecurityException {
        if (!enabled) {
            return;
        }
        if (validator.isAllowed(socket)) {
            allowedCount.incrementAndGet();
            return;
        }
        deniedCount.incrementAndGet();
        throw new SecurityException("remote address not allowed by policy " + name);
    }

    @Override
    public boolean allowed(final String addressOrCidr) {
        return validator.isAllowed(addressOrCidr);
    }

    private void rebuild() {
        var allow = CidrListLoader.load(allowList, name + " allowList");
        var deny = CidrListLoader.load(denyList, name + " denyList");
        allowListCount = allow.cidrs().size();
        denyListCount = deny.cidrs().size();
        allowListInvalidCount = allow.invalidCount();
        denyListInvalidCount = deny.invalidCount();
        validator = new RemoteAddressValidator(allow.cidrs(), deny.cidrs());
    }

    @Override
    public String getName() {
        return name;
    }

    /**
     * Names this policy instance in log messages and in its JMX object name.
     */
    public void setName(final String name) {
        this.name = name;
    }

    @Override
    public boolean isEnabled() {
        return enabled;
    }

    @Override
    public void setEnabled(final boolean enabled) {
        this.enabled = enabled;
    }

    @Override
    public String getAllowList() {
        return allowList;
    }

    /**
     * CIDR blocks that remote addresses must fall within to connect, as a comma
     * separated list (e.g. {@code 10.0.0.0/8,192.168.1.0/24}) or a {@code file:}
     * URI to a file with one CIDR block per line. {@code ${activemq.conf}} and
     * {@code ${activemq.data}} may be used in the URI. Empty means no restriction
     * beyond the deny list.
     */
    public void setAllowList(final String allowList) {
        this.allowList = allowList;
        rebuild();
    }

    @Override
    public String getDenyList() {
        return denyList;
    }

    /**
     * CIDR blocks that are refused regardless of the allow list, in the same
     * comma separated or {@code file:} URI form as the allow list. Deny entries
     * are checked first.
     */
    public void setDenyList(final String denyList) {
        this.denyList = denyList;
        rebuild();
    }

    @Override
    public long getAllowListCount() {
        return allowListCount;
    }

    @Override
    public long getDenyListCount() {
        return denyListCount;
    }

    @Override
    public long getAllowListInvalidCount() {
        return allowListInvalidCount;
    }

    @Override
    public long getDenyListInvalidCount() {
        return denyListInvalidCount;
    }

    @Override
    public long getAllowedCount() {
        return allowedCount.get();
    }

    @Override
    public long getDeniedCount() {
        return deniedCount.get();
    }

    @Override
    public void resetStatistics() {
        allowedCount.set(0L);
        deniedCount.set(0L);
    }

    @Override
    public String toString() {
        return "RemoteAddressConnectorPolicy{name=" + name + ", enabled=" + enabled
                + ", allowListCount=" + allowListCount + ", denyListCount=" + denyListCount
                + ", allowed=" + allowedCount.get() + ", denied=" + deniedCount.get() + "}";
    }

}
