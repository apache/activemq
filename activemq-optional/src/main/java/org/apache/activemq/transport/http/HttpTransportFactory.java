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
package org.apache.activemq.transport.http;

import org.activeio.command.WireFormat;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.transport.TransportServer;
import org.apache.activemq.transport.util.TextWireFormat;
import org.apache.activemq.transport.xstream.XStreamWireFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;

/**
 * @version $Revision$
 */
public class HttpTransportFactory extends TransportFactory {
    private static final Log log = LogFactory.getLog(HttpTransportFactory.class);

    public TransportServer doBind(String brokerId, URI location) throws IOException {
        return new HttpTransportServer(location);
    }

    protected TextWireFormat asTextWireFormat(WireFormat wireFormat) {
        if (wireFormat instanceof TextWireFormat) {
            return (TextWireFormat) wireFormat;
        }
        log.trace("Not created with a TextWireFormat: " + wireFormat);
        return new XStreamWireFormat();
    }

    protected String getDefaultWireFormatType() {
        return "xstream";
    }

    protected Transport createTransport(URI location, WireFormat wf) throws MalformedURLException {
        TextWireFormat textWireFormat = asTextWireFormat(wf);
        Transport transport = new HttpClientTransport(textWireFormat, location);
        return transport;
    }

}
