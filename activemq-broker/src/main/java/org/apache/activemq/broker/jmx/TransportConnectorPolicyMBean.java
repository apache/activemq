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
 * Common management view of a transport server policy. A policy configured on
 * a transport connector that implements this interface, or a subinterface such
 * as {@link RemoteAddressConnectorPolicyMBean}, is registered in JMX under the
 * connector's object name.
 */
public interface TransportConnectorPolicyMBean {

    @MBeanInfo("Name of this policy instance, used in its JMX object name")
    String getName();

    @MBeanInfo("Whether the policy is enforced")
    boolean isEnabled();

    @MBeanInfo("Enable or disable enforcement at runtime; the configuration stays loaded")
    void setEnabled(boolean enabled);

}
