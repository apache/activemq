/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
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
package org.apache.activemq.transport;

import org.apache.activemq.command.Command;
import org.apache.activemq.command.RemoveInfo;
import org.apache.activemq.command.ShutdownInfo;

import java.io.IOException;

/**
 * A useful base class for a transport implementation which has a background
 * reading thread.
 * 
 * @version $Revision: 1.1 $
 */
public abstract class TransportThreadSupport extends TransportSupport implements Runnable {

    private boolean daemon = false;
    private Thread runner;

    public boolean isDaemon() {
        return daemon;
    }

    public void setDaemon(boolean daemon) {
        this.daemon = daemon;
    }

    protected void doStart() throws Exception {
        runner = new Thread(this, toString());
        runner.setDaemon(daemon);
        runner.start();
    }

    protected void checkStarted(Command command) throws IOException {
        if (!isStarted()) {
            // we might try to shut down the transport before it was ever started in some test cases
            if (!(command instanceof ShutdownInfo || command instanceof RemoveInfo)) {
                throw new IOException("The transport " + this + " of type: " + getClass().getName() + " is not running.");
            }
        }
    }
}
