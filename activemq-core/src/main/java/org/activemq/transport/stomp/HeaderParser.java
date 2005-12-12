/*
 * Copyright (c) 2005 Your Corporation. All Rights Reserved.
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
