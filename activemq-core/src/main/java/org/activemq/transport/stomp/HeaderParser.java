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
package org.activemq.transport.stomp;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.IOException;
import java.net.ProtocolException;
import java.util.Properties;

class HeaderParser {
    /**
     * Reads headers up through the blank line
     * 
     * @param in
     * @return
     * @throws IOException
     */
    Properties parse(BufferedReader in) throws IOException {
        Properties props = new Properties();
        String line;
        while (((line = in.readLine()).trim().length() > 0)) {
            int seperator_index = line.indexOf(Stomp.Headers.SEPERATOR);
            String name = line.substring(0, seperator_index).trim();
            String value = line.substring(seperator_index + 1, line.length()).trim();
            props.setProperty(name, value);
        }
        return props;
    }

    Properties parse(DataInput in) throws IOException {
        Properties props = new Properties();
        String line;
        while (((line = in.readLine()).trim().length() > 0)) {
            try {
                int seperator_index = line.indexOf(Stomp.Headers.SEPERATOR);
                String name = line.substring(0, seperator_index).trim();
                String value = line.substring(seperator_index + 1, line.length()).trim();
                props.setProperty(name, value);
            }
            catch (Exception e) {
                throw new ProtocolException("Unable to parser header line [" + line + "]");
            }
        }
        return props;
    }
}
