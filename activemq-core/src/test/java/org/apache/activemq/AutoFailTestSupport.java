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

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;

import junit.framework.TestCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Enforces a test case to run for only an allotted time to prevent them from
 * hanging and breaking the whole testing.
 * 
 * @version $Revision: 1.0 $
 */

public abstract class AutoFailTestSupport extends TestCase {
    public static final int EXIT_SUCCESS = 0;
    public static final int EXIT_ERROR = 1;
    private static final Logger LOG = LoggerFactory.getLogger(AutoFailTestSupport.class);

    private long maxTestTime = 5 * 60 * 1000; // 5 mins by default
    private Thread autoFailThread;

    private boolean verbose = true;
    private boolean useAutoFail; // Disable auto fail by default
    private AtomicBoolean isTestSuccess;

    protected void setUp() throws Exception {
        // Runs the auto fail thread before performing any setup
        if (isAutoFail()) {
            startAutoFailThread();
        }
        super.setUp();
    }

    protected void tearDown() throws Exception {
        super.tearDown();

        // Stops the auto fail thread only after performing any clean up
        stopAutoFailThread();
    }

    /**
     * Manually start the auto fail thread. To start it automatically, just set
     * the auto fail to true before calling any setup methods. As a rule, this
     * method is used only when you are not sure, if the setUp and tearDown
     * method is propagated correctly.
     */
    public void startAutoFailThread() {
        setAutoFail(true);
        isTestSuccess = new AtomicBoolean(false);
        autoFailThread = new Thread(new Runnable() {
            public void run() {
                try {
                    // Wait for test to finish succesfully
                    Thread.sleep(getMaxTestTime());
                } catch (InterruptedException e) {
                    // This usually means the test was successful
                } finally {
                    // Check if the test was able to tear down succesfully,
                    // which usually means, it has finished its run.
                    if (!isTestSuccess.get()) {
                        LOG.error("Test case has exceeded the maximum allotted time to run of: " + getMaxTestTime() + " ms.");
                        dumpAllThreads(getName());
                        System.exit(EXIT_ERROR);
                    }
                }
            }
        }, "AutoFailThread");

        if (verbose) {
            LOG.info("Starting auto fail thread...");
        }

        LOG.info("Starting auto fail thread...");
        autoFailThread.start();
    }

    /**
     * Manually stops the auto fail thread. As a rule, this method is used only
     * when you are not sure, if the setUp and tearDown method is propagated
     * correctly.
     */
    public void stopAutoFailThread() {
        if (isAutoFail() && autoFailThread != null && autoFailThread.isAlive()) {
            isTestSuccess.set(true);

            if (verbose) {
                LOG.info("Stopping auto fail thread...");
            }

            LOG.info("Stopping auto fail thread...");
            autoFailThread.interrupt();
        }
    }

    /**
     * Sets the auto fail value. As a rule, this should be used only before any
     * setup methods is called to automatically enable the auto fail thread in
     * the setup method of the test case.
     * 
     * @param val
     */
    public void setAutoFail(boolean val) {
        this.useAutoFail = val;
    }

    public boolean isAutoFail() {
        return this.useAutoFail;
    }

    /**
     * The assigned value will only be reflected when the auto fail thread has
     * started its run. Value is in milliseconds.
     * 
     * @param val
     */
    public void setMaxTestTime(long val) {
        this.maxTestTime = val;
    }

    public long getMaxTestTime() {
        return this.maxTestTime;
    }
    
    
    public static void dumpAllThreads(String prefix) {
        Map<Thread, StackTraceElement[]> stacks = Thread.getAllStackTraces();
        for (Entry<Thread, StackTraceElement[]> stackEntry : stacks.entrySet()) {
            System.err.println(prefix + " " + stackEntry.getKey());
            for(StackTraceElement element : stackEntry.getValue()) {
                System.err.println("     " + element);
            }
        }
    }
}
