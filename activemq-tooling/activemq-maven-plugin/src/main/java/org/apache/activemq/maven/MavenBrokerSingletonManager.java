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
package org.apache.activemq.maven;

import org.apache.activemq.broker.BrokerService;
import org.apache.maven.plugin.MojoExecutionException;

/**
 * Broker manager for use with Maven; uses the original singleton to track the broker instance.
 */
public class MavenBrokerSingletonManager implements MavenBrokerManager {

    public void start(boolean fork, String configUri) throws MojoExecutionException {
        Broker.start(fork, configUri);
    }

    public void stop() throws MojoExecutionException {
        Broker.stop();
    }

    /**
     * Wait for a shutdown invocation elsewhere
     *
     * @throws Exception
     */
    protected void waitForShutdown() throws Exception {
        Broker.waitForShutdown();
    }

    /**
     * Return the broker service created.
     */
    public BrokerService getBroker() {
        return Broker.getBroker();
    }

    /**
     * Override the default creation of the broker service.  Primarily added for testing purposes.
     *
     * @param broker
     */
    public void setBroker(BrokerService broker) {
        Broker.setBroker(broker);
    }
}
