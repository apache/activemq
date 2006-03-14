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

import org.activeio.ByteArrayInputStream;
import org.apache.activemq.command.Command;
import org.apache.activemq.command.LastPartialCommand;
import org.apache.activemq.command.PartialCommand;
import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;

/**
 * Joins together of partial commands which were split into individual chunks of
 * data.
 * 
 * @version $Revision$
 */
public class CommandJoiner extends TransportFilter {
    private static final Log log = LogFactory.getLog(CommandJoiner.class);

    private ByteArrayOutputStream out = new ByteArrayOutputStream();
    private OpenWireFormat wireFormat;

    public CommandJoiner(Transport next, OpenWireFormat wireFormat) {
        super(next);
        this.wireFormat = wireFormat;
    }

    public void onCommand(Command command) {
        byte type = command.getDataStructureType();
        if (type == PartialCommand.DATA_STRUCTURE_TYPE) {
            PartialCommand header = (PartialCommand) command;
            byte[] partialData = header.getData();
            try {
                out.write(partialData);
            }
            catch (IOException e) {
                getTransportListener().onException(e);
            }
        }
        else if (type == LastPartialCommand.DATA_STRUCTURE_TYPE) {
            try {
                byte[] fullData = out.toByteArray();
                out.reset();
                Command completeCommand = (Command) wireFormat.unmarshal(new DataInputStream(new ByteArrayInputStream(fullData)));
                
                LastPartialCommand lastCommand = (LastPartialCommand) command;
                lastCommand.configure(completeCommand);
                
                getTransportListener().onCommand(completeCommand);
            }
            catch (IOException e) {
                log.warn("Failed to unmarshal partial command: " + command);
                getTransportListener().onException(e);
            }
        }
        else {
            getTransportListener().onCommand(command);
        }
    }

    public void stop() throws Exception {
        super.stop();
        out = null;
    }

    public String toString() {
        return next.toString();
    }
}
