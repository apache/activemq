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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class DefaultIOExceptionHandler implements IOExceptionHandler {

    private static final Log LOG = LogFactory
            .getLog(DefaultIOExceptionHandler.class);
    private BrokerService broker;
    private boolean ignoreAllErrors = false;

    public void handle(IOException exception) {
        if (ignoreAllErrors) {
            LOG.info("Ignoring IO exception, " + exception, exception);
            return;
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

}
