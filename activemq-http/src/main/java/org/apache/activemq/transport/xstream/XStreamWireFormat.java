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
package org.apache.activemq.transport.xstream;

import java.io.IOException;
import java.io.Reader;

import org.apache.activemq.command.MarshallAware;
import org.apache.activemq.command.MessageDispatch;
import org.apache.activemq.transport.util.TextWireFormat;
import org.apache.activemq.wireformat.WireFormat;

import com.thoughtworks.xstream.XStream;

/**
 * A {@link WireFormat} implementation which uses the <a
 * href="http://xstream.codehaus.org/>XStream</a> library to marshall commands
 * onto the wire
 *
 *
 */
public class XStreamWireFormat extends TextWireFormat {
    private XStream xStream;
    private int version;

    @Override
    public int getVersion() {
        return version;
    }

    @Override
    public void setVersion(int version) {
        this.version = version;
    }

    public WireFormat copy() {
        return new XStreamWireFormat();
    }

    @Override
    public Object unmarshalText(String text) {
        return getXStream().fromXML(text);
    }

    @Override
    public Object unmarshalText(Reader reader) {
        return getXStream().fromXML(reader);
    }

    @Override
    public String marshalText(Object command) throws IOException {
        if (command instanceof MarshallAware) {
            ((MarshallAware)command).beforeMarshall(this);
        } else if(command instanceof MessageDispatch) {
            MessageDispatch dispatch = (MessageDispatch) command;
            if (dispatch != null && dispatch.getMessage() != null) {
                dispatch.getMessage().beforeMarshall(this);
            }
        }

        return getXStream().toXML(command);
    }

    /**
     * Can this wireformat process packets of this version
     *
     * @param version the version number to test
     * @return true if can accept the version
     */
    public boolean canProcessWireFormatVersion(int version) {
        return true;
    }

    /**
     * @return the current version of this wire format
     */
    public int getCurrentWireFormatVersion() {
        return 1;
    }

    // Properties
    // -------------------------------------------------------------------------
    public XStream getXStream() {
        if (xStream == null) {
            xStream = createXStream();
            // make it work in OSGi env
            xStream.setClassLoader(getClass().getClassLoader());
        }
        return xStream;
    }

    public void setXStream(XStream xStream) {
        this.xStream = xStream;
    }

    // Implementation methods
    // -------------------------------------------------------------------------
    protected XStream createXStream() {
        XStream xstream = new XStream();
        xstream.ignoreUnknownElements();
        return xstream;
    }

}
