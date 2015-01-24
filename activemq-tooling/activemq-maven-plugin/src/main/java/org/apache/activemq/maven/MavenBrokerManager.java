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
 * Manager of the broker used by the maven plugin.
 */
public interface MavenBrokerManager {
    /**
     * Start the broker using the fork setting and configuration at the given URI.
     *
     * @param fork true => run the broker asynchronously; false => run the broker synchronously (this method does not
     *             return until the broker shuts down)
     * @param configUri URI of the broker configuration; prefix with "xbean:file" to read XML configuration from a file.
     * @throws MojoExecutionException
     */
    void start(boolean fork, String configUri) throws MojoExecutionException;

    /**
     * Stop the broker.
     *
     * @throws MojoExecutionException
     */
    void stop() throws MojoExecutionException;

    /**
     * Return the broker service created.
     */
    BrokerService getBroker();

    /**
     * Set the broker service managed to the one given.
     *
     * @param broker activemq instance to manage.
     */
    void setBroker(BrokerService broker);
}
