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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * @version $Revision$
 */
public class TransportLogger extends TransportFilter {

    private static int lastId=0;
    private final Log log;
    
    public TransportLogger(Transport next) {
        this( next, LogFactory.getLog(TransportLogger.class.getName()+"."+getNextId()));
    }
    
    synchronized private static int getNextId() {
        return ++lastId;
    }

    public TransportLogger(Transport next, Log log) {
        super(next);
        this.log = log;
    }

    public void oneway(Command command) throws IOException {
        if( log.isDebugEnabled() ) {
            log.debug("SENDING: "+command);
        }
        next.oneway(command);
    }
    
    public String toString() {
        return next.toString();
    }
}
