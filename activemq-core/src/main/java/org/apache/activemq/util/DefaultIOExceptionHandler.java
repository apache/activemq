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

import org.apache.activemq.broker.BrokerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultIOExceptionHandler implements IOExceptionHandler {

    private static final Logger LOG = LoggerFactory
            .getLogger(DefaultIOExceptionHandler.class);
    private BrokerService broker;
    private boolean ignoreAllErrors = false;
    private boolean ignoreNoSpaceErrors = true;
    private String noSpaceMessage = "space";

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

        LOG.info("Stopping the broker due to IO exception, " + exception, exception);
        new Thread() {
            public void run() {
                try {
                    broker.stop();
                } catch (Exception e) {
                    LOG.warn("Failure occured while stopping broker", e);
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

}
