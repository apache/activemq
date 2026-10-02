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
package org.apache.activemq;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Arrays;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.Assert.assertTrue;

import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.plugin.RuntimeConfigurationBroker;
import org.apache.activemq.spring.Utils;
import org.apache.activemq.util.Wait;
import org.junit.After;
import org.junit.Rule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.Resource;

public class RuntimeConfigTestSupport {
    public static final Logger LOG = LoggerFactory.getLogger(RuntimeConfigTestSupport.class);

    public static final int SLEEP = 4; // seconds
    /** longest applyNewConfig waits for a running broker to process a changed file */
    public static final long MAX_CHANGE_WAIT_SECONDS = 30;
    public static final boolean WAIT_FOR_CHANGE = true;
    public static final String EMPTY_UPDATABLE_CONFIG = "emptyUpdatableConfig1000" ;
    protected BrokerService brokerService;

    @Rule
    public TestWatcher watchman = new TestWatcher() {
        @Override
        public void starting(Description description) {
          LOG.info("{} being run...", description.getMethodName());
        }
    };

    public void startBroker(String configFileName) throws Exception {
        brokerService = createBroker(configFileName);
        brokerService.start();
        brokerService.waitUntilStarted();
    }

    public BrokerService createBroker(String configFileName) throws Exception {
        brokerService = new BrokerService();
        return BrokerFactory.createBroker("xbean:org/apache/activemq/" + configFileName + ".xml");
    }

    protected void applyNewConfig(String configName, String newConfigName) throws Exception {
        applyNewConfig(configName, newConfigName, false);
    }

    /**
     * Copies {@code newConfigName.xml} over {@code configName.xml}. With
     * {@code waitForChange} the call returns once the running broker's
     * {@link RuntimeConfigurationBroker} has processed the new file, or at once
     * when nothing is polling it (manual update mode, or no broker started).
     */
    protected void applyNewConfig(String configName, String newConfigName, boolean waitForChange) throws Exception {
        Resource resource = Utils.resourceFromString("org/apache/activemq");
        File file = new File(resource.getFile(), configName + ".xml");
        long previous = file.lastModified();
        FileOutputStream current = new FileOutputStream(file);
        FileInputStream modifications = new FileInputStream(new File(resource.getFile(), newConfigName + ".xml"));
        modifications.getChannel().transferTo(0, Long.MAX_VALUE, current.getChannel());
        current.flush();
        current.getChannel().force(true);
        current.close();
        modifications.close();
        long modified = bumpLastModified(file, previous);
        LOG.info("Updated: " + file + " (" + modified + ") " + new Date(modified));

        if (waitForChange) {
            waitForChange(file, modified);
        }
    }

    /**
     * The monitor only reloads when the file's time moves forward, and JMX reports
     * that time to the second, so move it at least a second past what the broker
     * last saw. Some file systems round to two seconds; keep going until the value
     * read back is later than the previous one.
     */
    private static long bumpLastModified(File file, long previous) {
        long modified = Math.max(System.currentTimeMillis(), previous + 1001);
        for (int attempt = 0; attempt < 10; attempt++) {
            assertTrue("could not set modification time on " + file, file.setLastModified(modified));
            long readBack = file.lastModified();
            if (readBack > previous) {
                return readBack;
            }
            modified += 1000;
        }
        throw new AssertionError("modification time on " + file + " will not advance past " + previous);
    }

    private void waitForChange(File file, long modified) throws Exception {
        RuntimeConfigurationBroker plugin = runtimeConfigurationBroker();
        if (plugin == null || plugin.getCheckPeriod() <= 0) {
            return;
        }
        boolean processed = Wait.waitFor(() -> plugin.getLastChecked() >= modified, TimeUnit.SECONDS.toMillis(MAX_CHANGE_WAIT_SECONDS), 10);
        if (!processed) {
            // the monitor runs on the broker scheduler thread; show what it was doing
            Thread.getAllStackTraces().forEach((thread, stack) -> {
                if (thread.getName().contains("Scheduler")) {
                    LOG.error("{} state {}\n\t{}", thread.getName(), thread.getState(),
                            Arrays.stream(stack).map(Object::toString).collect(Collectors.joining("\n\t")));
                }
            });
        }
        assertTrue("configuration change not processed within " + MAX_CHANGE_WAIT_SECONDS + "s; expected " + modified
                + ", file " + file.lastModified() + ", broker lastModified " + plugin.getLastModified()
                + ", lastChecked " + plugin.getLastChecked() + ", checkPeriod " + plugin.getCheckPeriod(), processed);
    }

    private RuntimeConfigurationBroker runtimeConfigurationBroker() {
        if (brokerService == null || !brokerService.isStarted()) {
            return null;
        }
        try {
            return (RuntimeConfigurationBroker) brokerService.getBroker().getAdaptor(RuntimeConfigurationBroker.class);
        } catch (Exception e) {
            LOG.debug("No runtime configuration plugin on the broker", e);
            return null;
        }
    }

    @After
    public void stopBroker() throws Exception {
        brokerService.stop();
        brokerService.waitUntilStopped();
    }
}
