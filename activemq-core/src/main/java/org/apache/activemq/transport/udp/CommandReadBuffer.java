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
package org.apache.activemq.transport.udp;

import org.apache.activemq.command.Command;
import org.apache.activemq.openwire.OpenWireFormat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Buffers up incoming headers to reorder them. This class is only accessed by
 * one thread at once.
 * 
 * @version $Revision$
 */
public class CommandReadBuffer {

    private OpenWireFormat wireFormat;
    private SortedSet headers = new TreeSet();
    private int expectedCounter;
    private ByteArrayOutputStream out = new ByteArrayOutputStream();

    public CommandReadBuffer(OpenWireFormat wireFormat) {
        this.wireFormat = wireFormat;
    }

    public Command read(DatagramHeader header) throws IOException {
        if (expectedCounter != header.getCounter()) {
            // lets add it to the list for later on
            headers.add(header);

            // lets see if the first item in the set is the next header
            header = (DatagramHeader) headers.first();
            if (expectedCounter != header.getCounter()) {
                return null;
            }
        }

        // we've got a valid header so increment counter
        expectedCounter++;

        Command answer = null;
        if (!header.isPartial()) {
            answer = header.getCommand();
            if (answer == null) {
                throw new IllegalStateException("The header should have a command!: " + header);
            }
        }
        else {
            byte[] data = header.getPartialData();
            out.write(data);

            if (header.isComplete()) {
                answer = (Command) wireFormat.unmarshal(new DataInputStream(new ByteArrayInputStream(out.toByteArray())));
                out.reset();
            }
        }
        return answer;

    }

}
