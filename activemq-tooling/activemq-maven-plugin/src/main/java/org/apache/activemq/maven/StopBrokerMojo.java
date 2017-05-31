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

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;

/**
 * Goal which stops an activemq broker.
 *
 * @goal stop
 * @phase process-sources
 */
public class StopBrokerMojo extends AbstractMojo {
    private MavenBrokerManager  brokerManager;

    public MavenBrokerManager getBrokerManager() {
        return brokerManager;
    }

    public void setBrokerManager(MavenBrokerManager brokerManager) {
        this.brokerManager = brokerManager;
    }

    /**
     * Skip execution of the ActiveMQ Broker plugin if set to true
     *
     * @parameter property="skip"
     */
    private boolean skip;

    public void execute() throws MojoExecutionException {
        if (skip) {
            getLog().info("Skipped execution of ActiveMQ Broker");
            return;
        }

        this.useBrokerManager().stop();

        getLog().info("Stopped the ActiveMQ Broker");
    }

    protected MavenBrokerManager    useBrokerManager () {
        if ( this.brokerManager == null ) {
            this.brokerManager = new MavenBrokerSingletonManager();
        }

        return  this.brokerManager;
    }
}
