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

import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.maven.plugin.MojoExecutionException;

/**
 * Singleton facade between Maven and one ActiveMQ broker.
 */
public class Broker {

    private static BrokerService broker;

    private static boolean[] shutdown;

    private static Thread shutdownThread;

    public static void start(boolean fork, String configUri) throws MojoExecutionException {

        if (broker != null) {
            throw new MojoExecutionException("A local broker is already running");
        }

        try {
            broker = BrokerFactory.createBroker(configUri);
            if (fork) {
                new Thread(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            broker.start();
                            waitForShutdown();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }).start();
            } else {
                broker.start();
                waitForShutdown();
            }
        } catch (Exception e) {
            throw new MojoExecutionException("Failed to start the ActiveMQ Broker", e);
        }
    }

    public static void stop() throws MojoExecutionException {

        if (broker == null) {
            throw new MojoExecutionException("The local broker is not running");
        }

        try {
            broker.stop();
            broker.waitUntilStopped();
            broker = null;

            Runtime.getRuntime().removeShutdownHook(shutdownThread);

            // Terminate the shutdown hook thread
            synchronized (shutdown) {
                shutdown[0] = true;
                shutdown.notify();
            }
        } catch (Exception e) {
            throw new MojoExecutionException("Failed to stop the ActiveMQ Broker", e);
        }
    }

    /**
     * Wait for a shutdown invocation elsewhere
     *
     * @throws Exception
     */
    protected static void waitForShutdown() throws Exception {
        shutdown = new boolean[] { false };

        shutdownThread = new Thread() {
            @Override
            public void run() {
                synchronized (shutdown) {
                    shutdown[0] = true;
                    shutdown.notify();
                }
            }
        };

        Runtime.getRuntime().addShutdownHook(shutdownThread);

        // Wait for any shutdown event
        synchronized (shutdown) {
            while (!shutdown[0]) {
                try {
                    shutdown.wait();
                } catch (InterruptedException e) {
                }
            }
        }

        // Stop broker
        if (broker != null) {
            broker.stop();
        }
    }

    /**
     * Return the broker service created.
     */
    public static BrokerService getBroker() {
        return broker;
    }

    /**
     * Override the default creation of the broker service.  Primarily added for testing purposes.
     *
     * @param broker
     */
    public static void setBroker(BrokerService broker) {
        Broker.broker = broker;
    }
}
