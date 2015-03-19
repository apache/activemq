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
package org.apache.activemq.transport.amqp.client;

import org.apache.qpid.proton.engine.Connection;
import org.apache.qpid.proton.engine.Receiver;
import org.apache.qpid.proton.engine.Sender;
import org.apache.qpid.proton.engine.Session;

/**
 * Abstract base for a validation hook that is used in tests to check
 * the state of a remote resource after a variety of lifecycle events.
 */
public class AmqpValidator {

    private boolean valid = true;
    private String errorMessage;

    public void inspectOpenedResource(Connection connection) {

    }

    public void inspectOpenedResource(Session session) {

    }

    public void inspectOpenedResource(Sender sender) {

    }

    public void inspectOpenedResource(Receiver receiver) {

    }

    public void inspectClosedResource(Connection remoteConnection) {

    }

    public void inspectClosedResource(Session session) {

    }

    public void inspectClosedResource(Sender sender) {

    }

    public void inspectClosedResource(Receiver receiver) {

    }

    public void inspectDetachedResource(Sender sender) {

    }

    public void inspectDetachedResource(Receiver receiver) {

    }

    public boolean isValid() {
        return valid;
    }

    protected void setValid(boolean valid) {
        this.valid = valid;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    protected void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    protected void markAsInvalid(String errorMessage) {
        if (valid) {
            setValid(false);
            setErrorMessage(errorMessage);
        }
    }

    public void assertValid() {
        if (!isValid()) {
            throw new AssertionError(errorMessage);
        }
    }
}
