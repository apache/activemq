/**
 *
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.activemq.transport;

import java.io.IOException;

import org.activemq.command.Command;
import org.activemq.command.Response;


/**
 * @version $Revision$
 */
public class MutexTransport extends TransportFilter {

    private final Object writeMutex = new Object();
    
    public MutexTransport(Transport next) {
        super(next);
    }

    public FutureResponse asyncRequest(Command command) throws IOException {
        synchronized(writeMutex) {
            return next.asyncRequest(command);
        }
    }

    public void oneway(Command command) throws IOException {
        synchronized(writeMutex) {
            next.oneway(command);
        }
    }

    public Response request(Command command) throws IOException {
        synchronized(writeMutex) {
            return next.request(command);
        }
    }
    
    public String toString() {
        return next.toString();
    }
    
}