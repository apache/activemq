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
package org.apache.activemq.util;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.activemq.broker.BrokerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @org.apache.xbean.XBean
 */
 public class DefaultIOExceptionHandler implements IOExceptionHandler {

    private static final Logger LOG = LoggerFactory
            .getLogger(DefaultIOExceptionHandler.class);
    private BrokerService broker;
    private boolean ignoreAllErrors = false;
    private boolean ignoreNoSpaceErrors = true;
    private boolean ignoreSQLExceptions = true;
    private boolean stopStartConnectors = false;
    private String noSpaceMessage = "space";
    private String sqlExceptionMessage = ""; // match all
    private long resumeCheckSleepPeriod = 5*1000;
    private AtomicBoolean stopStartInProgress = new AtomicBoolean(false);

    public void handle(IOException exception) {
        if (ignoreAllErrors) {
            LOG.info("Ignoring IO exception, " + exception, exception);
            return;
        }

        if (ignoreNoSpaceErrors) {
            Throwable cause = exception;
            while (cause != null && cause instanceof IOException) {
                if (cause.getMessage().contains(noSpaceMessage)) {
                    LOG.info("Ignoring no space left exception, " + exception, exception);
                    return;
                }
                cause = cause.getCause();
            }
        }

        if (ignoreSQLExceptions) {
            Throwable cause = exception;
            while (cause != null) {
                if (cause instanceof SQLException && cause.getMessage().contains(sqlExceptionMessage)) {
                    LOG.info("Ignoring SQLException, " + exception, cause);
                    return;
                }
                cause = cause.getCause();
            }
        }

        if (stopStartConnectors) {
            if (!stopStartInProgress.compareAndSet(false, true)) {
                // we are already working on it
                return;
            }
            LOG.info("Initiating stop/restart of broker transport due to IO exception, " + exception, exception);

            new Thread("stop transport connectors on IO exception") {
                public void run() {
                    try {
                        ServiceStopper stopper = new ServiceStopper();
                        broker.stopAllConnectors(stopper);
                    } catch (Exception e) {
                        LOG.warn("Failure occurred while stopping broker connectors", e);
                    }
                }
            }.start();

            // resume again
            new Thread("restart transport connectors post IO exception") {
                public void run() {
                    try {
                        while (isPersistenceAdapterDown()) {
                            LOG.info("waiting for broker persistence adapter checkpoint to succeed before restarting transports");
                            TimeUnit.MILLISECONDS.sleep(resumeCheckSleepPeriod);
                        }
                        broker.startAllConnectors();
                    } catch (Exception e) {
                        LOG.warn("Failure occurred while restarting broker connectors", e);
                    } finally {
                        stopStartInProgress.compareAndSet(true, false);
                    }
                }

                private boolean isPersistenceAdapterDown() {
                    boolean checkpointSuccess = false;
                    try {
                        broker.getPersistenceAdapter().checkpoint(true);
                        checkpointSuccess = true;
                    } catch (Throwable ignored) {}
                    return !checkpointSuccess;
                }
            }.start();

            return;
        }

        LOG.info("Stopping the broker due to IO exception, " + exception, exception);
        new Thread("Stopping the broker due to IO exception") {
            public void run() {
                try {
                    broker.stop();
                } catch (Exception e) {
                    LOG.warn("Failure occurred while stopping broker", e);
                }
            }
        }.start();
    }

    public void setBrokerService(BrokerService broker) {
        this.broker = broker;
    }

    public boolean isIgnoreAllErrors() {
        return ignoreAllErrors;
    }

    public void setIgnoreAllErrors(boolean ignoreAllErrors) {
        this.ignoreAllErrors = ignoreAllErrors;
    }

    public boolean isIgnoreNoSpaceErrors() {
        return ignoreNoSpaceErrors;
    }

    public void setIgnoreNoSpaceErrors(boolean ignoreNoSpaceErrors) {
        this.ignoreNoSpaceErrors = ignoreNoSpaceErrors;
    }

    public String getNoSpaceMessage() {
        return noSpaceMessage;
    }

    public void setNoSpaceMessage(String noSpaceMessage) {
        this.noSpaceMessage = noSpaceMessage;
    }

    public boolean isIgnoreSQLExceptions() {
        return ignoreSQLExceptions;
    }

    public void setIgnoreSQLExceptions(boolean ignoreSQLExceptions) {
        this.ignoreSQLExceptions = ignoreSQLExceptions;
    }

    public String getSqlExceptionMessage() {
        return sqlExceptionMessage;
    }

    public void setSqlExceptionMessage(String sqlExceptionMessage) {
        this.sqlExceptionMessage = sqlExceptionMessage;
    }

    public boolean isStopStartConnectors() {
        return stopStartConnectors;
    }

    public void setStopStartConnectors(boolean stopStartConnectors) {
        this.stopStartConnectors = stopStartConnectors;
    }

    public long getResumeCheckSleepPeriod() {
        return resumeCheckSleepPeriod;
    }

    public void setResumeCheckSleepPeriod(long resumeCheckSleepPeriod) {
        this.resumeCheckSleepPeriod = resumeCheckSleepPeriod;
    }
}
