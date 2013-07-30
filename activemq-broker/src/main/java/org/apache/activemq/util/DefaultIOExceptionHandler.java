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
import org.apache.activemq.broker.SuppressReplyException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @org.apache.xbean.XBean
 */
 public class DefaultIOExceptionHandler implements IOExceptionHandler {

    private static final Logger LOG = LoggerFactory
            .getLogger(DefaultIOExceptionHandler.class);
    protected BrokerService broker;
    private boolean ignoreAllErrors = false;
    private boolean ignoreNoSpaceErrors = true;
    private boolean ignoreSQLExceptions = true;
    private boolean stopStartConnectors = false;
    private String noSpaceMessage = "space";
    private String sqlExceptionMessage = ""; // match all
    private long resumeCheckSleepPeriod = 5*1000;
    private AtomicBoolean handlingException = new AtomicBoolean(false);

    public void handle(IOException exception) {
        if (ignoreAllErrors) {
            LOG.info("Ignoring IO exception, " + exception, exception);
            return;
        }

        if (ignoreNoSpaceErrors) {
            Throwable cause = exception;
            while (cause != null && cause instanceof IOException) {
                String message = cause.getMessage();
                if (message != null && message.contains(noSpaceMessage)) {
                    LOG.info("Ignoring no space left exception, " + exception, exception);
                    return;
                }
                cause = cause.getCause();
            }
        }

        if (ignoreSQLExceptions) {
            Throwable cause = exception;
            while (cause != null) {
                String message = cause.getMessage();
                if (cause instanceof SQLException && message.contains(sqlExceptionMessage)) {
                    LOG.info("Ignoring SQLException, " + exception, cause);
                    return;
                }
                cause = cause.getCause();
            }
        }

        if (stopStartConnectors) {
            if (handlingException.compareAndSet(false, true)) {
                LOG.info("Initiating stop/restart of transports on " + broker + " due to IO exception, " + exception, exception);

                new Thread("IOExceptionHandler: stop transports") {
                    public void run() {
                        try {
                            ServiceStopper stopper = new ServiceStopper();
                            broker.stopAllConnectors(stopper);
                            LOG.info("Successfully stopped transports on " + broker);
                        } catch (Exception e) {
                            LOG.warn("Failure occurred while stopping broker connectors", e);
                        } finally {
                            // resume again
                            new Thread("IOExceptionHandler: restart transports") {
                                public void run() {
                                    try {
                                        while (hasLockOwnership() && isPersistenceAdapterDown()) {
                                            LOG.info("waiting for broker persistence adapter checkpoint to succeed before restarting transports");
                                            TimeUnit.MILLISECONDS.sleep(resumeCheckSleepPeriod);
                                        }
                                        broker.startAllConnectors();
                                        LOG.info("Successfully restarted transports on " + broker);
                                    } catch (Exception e) {
                                        LOG.warn("Stopping " + broker + " due to failure while restarting transports", e);
                                        stopBroker(e);
                                    } finally {
                                        handlingException.compareAndSet(true, false);
                                    }
                                }

                                private boolean isPersistenceAdapterDown() {
                                    boolean checkpointSuccess = false;
                                    try {
                                        broker.getPersistenceAdapter().checkpoint(true);
                                        checkpointSuccess = true;
                                    } catch (Throwable ignored) {
                                    }
                                    return !checkpointSuccess;
                                }
                            }.start();


                        }
                    }
                }.start();
            }

            throw new SuppressReplyException("Stop/RestartTransportsInitiated", exception);
        }

        if (handlingException.compareAndSet(false, true)) {
            stopBroker(exception);
        }

        // we don't want to propagate the exception back to the client
        // They will see a delay till they see a disconnect via socket.close
        // at which point failover: can kick in.
        throw new SuppressReplyException("ShutdownBrokerInitiated", exception);
    }

    private void stopBroker(Exception exception) {
        LOG.info("Stopping " + broker + " due to exception, " + exception, exception);
        new Thread("IOExceptionHandler: stopping " + broker) {
            public void run() {
                try {
                    if( broker.isRestartAllowed() ) {
                        broker.requestRestart();
                    }
                    broker.stop();
                } catch (Exception e) {
                    LOG.warn("Failure occurred while stopping broker", e);
                }
            }
        }.start();
    }

    protected boolean hasLockOwnership() throws IOException {
        return true;
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
