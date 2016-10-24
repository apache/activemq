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

import org.apache.qpid.proton.amqp.Binary;
import org.apache.qpid.proton.amqp.transport.Attach;
import org.apache.qpid.proton.amqp.transport.Begin;
import org.apache.qpid.proton.amqp.transport.Close;
import org.apache.qpid.proton.amqp.transport.Detach;
import org.apache.qpid.proton.amqp.transport.Disposition;
import org.apache.qpid.proton.amqp.transport.End;
import org.apache.qpid.proton.amqp.transport.Flow;
import org.apache.qpid.proton.amqp.transport.Open;
import org.apache.qpid.proton.amqp.transport.Transfer;

/**
 * Abstract base for a validation hook that is used in tests to check
 * the state of a remote resource after a variety of lifecycle events.
 */
public class AmqpFrameValidator {

    private boolean valid = true;
    private String errorMessage;

    public void inspectOpen(Open open, Binary encoded) {

    }

    public void inspectBegin(Begin begin, Binary encoded) {

    }

    public void inspectAttach(Attach attach, Binary encoded) {

    }

    public void inspectFlow(Flow flow, Binary encoded) {

    }

    public void inspectTransfer(Transfer transfer, Binary encoded) {

    }

    public void inspectDisposition(Disposition disposition, Binary encoded) {

    }

    public void inspectDetach(Detach detach, Binary encoded) {

    }

    public void inspectEnd(End end, Binary encoded) {

    }

    public void inspectClose(Close close, Binary encoded) {

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
