/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.transport.xmpp;

import ietf.params.xml.ns.xmpp_sasl.Mechanisms;
import org.apache.activemq.command.BrokerInfo;
import org.apache.activemq.command.Command;
import org.apache.activemq.transport.tcp.TcpBufferedInputStream;
import org.apache.activemq.transport.tcp.TcpBufferedOutputStream;
import org.apache.activemq.transport.tcp.TcpTransport;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.ServiceStopper;
import org.apache.activemq.wireformat.WireFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jabber.etherx.streams.Features;

import javax.net.SocketFactory;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.namespace.QName;
import javax.xml.stream.Location;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLReporter;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.URI;

/**
 * @version $Revision$
 */
public class XmppTransport extends TcpTransport {
    protected static final QName ATTRIBUTE_TO = new QName("to");

    private static final transient Log log = LogFactory.getLog(XmppTransport.class);

    private JAXBContext context;
    private XMLEventReader xmlReader;
    private Unmarshaller unmarshaller;
    private Marshaller marshaller;
    private XMLStreamWriter xmlWriter;
    private String to = "client";
    protected OutputStream outputStream;
    protected InputStream inputStream;
    private ProtocolConverter converter;
    private String from;

    public XmppTransport(WireFormat wireFormat, Socket socket) throws IOException {
        super(wireFormat, socket);
        init();
    }

    public XmppTransport(WireFormat wireFormat, SocketFactory socketFactory, URI uri, URI uri1) throws IOException {
        super(wireFormat, socketFactory, uri, uri1);
        init();
    }

    private void init() {
        converter = new ProtocolConverter(this);
    }


    @Override
    public void oneway(Object object) throws IOException {
        if (object instanceof Command) {
            Command command = (Command) object;

            if (command instanceof BrokerInfo) {
                BrokerInfo brokerInfo = (BrokerInfo) command;

                String id = brokerInfo.getBrokerId().toString();
                from = brokerInfo.getBrokerName();
                try {
                    writeOpenStream(id, from);

                    // now lets write the features
                    Features features = new Features();
                    //features.getAny().add(new Starttls());
                    Mechanisms mechanisms = new Mechanisms();
                    //mechanisms.getMechanism().add("DIGEST-MD5");
                    //mechanisms.getMechanism().add("PLAIN");
                    features.getAny().add(mechanisms);
                    marshall(features);
                    /*
                    xmlWriter.flush();
                    outputStream.flush();
                    */
                }
                catch (XMLStreamException e) {
                    throw IOExceptionSupport.create(e);
                }
            }
            else {
                try {
                    converter.onActiveMQCommad(command);
                }
                catch (IOException e) {
                    throw e;
                }
                catch (Exception e) {
                    throw IOExceptionSupport.create(e);
                }
            }
        }
        else {
            log.warn("Unkown command: " + object);
        }
    }


    /**
     * Marshalls the given POJO to the client
     */
    public void marshall(Object command) throws IOException {
        try {
            marshaller.marshal(command, xmlWriter);
            xmlWriter.flush();
        }
        catch (JAXBException e) {
            throw IOExceptionSupport.create(e);
        }
        catch (XMLStreamException e) {
            throw IOExceptionSupport.create(e);
        }
    }

    @Override
    public void run() {
        log.debug("XMPP consumer thread starting");

        try {
            XMLInputFactory xif = XMLInputFactory.newInstance();
            xif.setXMLReporter(new XMLReporter() {
                public void report(String message, String errorType, Object relatedInformation, Location location) throws XMLStreamException {
                    log.warn(message + " errorType: " + errorType + " relatedInfo: " + relatedInformation);
                }
            });

            xmlReader = xif.createXMLEventReader(inputStream);

            XMLEvent docStart = xmlReader.nextEvent();

            XMLEvent rootElement = xmlReader.nextTag();

            if (rootElement instanceof StartElement) {
                StartElement startElement = (StartElement) rootElement;
                Attribute toAttribute = startElement.getAttributeByName(ATTRIBUTE_TO);
                if (toAttribute != null) {
                    to = toAttribute.getValue();
                }
            }
            while (true) {
                if (isStopped()) {
                    break;
                }

                XMLEvent event = xmlReader.peek();
                if (event.isStartElement()) {
                    // unmarshal a new object
                    Object object = unmarshaller.unmarshal(xmlReader);
                    if (object != null) {
                        converter.onXmppCommand(object);
                    }
                }
                else {
                    if (event.getEventType() == XMLEvent.END_ELEMENT) {
                        break;
                    }
                    else if (event.getEventType() == XMLEvent.END_ELEMENT || event.getEventType() == XMLEvent.END_DOCUMENT) {
                        break;
                    }
                    else {
                        xmlReader.nextEvent();
                    }

                }
            }
        }
        catch (XMLStreamException e) {
            log.error("XMPP Reader thread caught: " + e, e);
        }
        catch (Exception e) {
            log.error("XMPP Reader thread caught: " + e, e);
        }
        try {
            stop();
        }
        catch (Exception e) {
            log.error("Failed to stop XMPP transport: " + e, e);
        }
    }


    public String getFrom() {
        return from;
    }

    @Override
    protected void doStop(ServiceStopper stopper) throws Exception {
        if (xmlWriter != null) {
            try {
                xmlWriter.writeEndElement();
                xmlWriter.writeEndDocument();
                xmlWriter.close();
            }
            catch (XMLStreamException e) {
                // the client may have closed first so ignore this
                log.info("Caught trying to close transport: " + e, e);
            }
        }
        if (xmlReader != null) {
            try {
                xmlReader.close();
            }
            catch (XMLStreamException e) {
                // the client may have closed first so ignore this
                log.info("Caught trying to close transport: " + e, e);
            }
        }
        super.doStop(stopper);
    }

    @Override
    protected void initializeStreams() throws Exception {
        // TODO it would be preferable to use class discovery here!
        context = JAXBContext.newInstance("jabber.client" + ":jabber.server"
                + ":jabber.iq._private" + ":jabber.iq.auth" + ":jabber.iq.gateway" + ":jabber.iq.last" + ":jabber.iq.oob"
                + ":jabber.iq.pass" + ":jabber.iq.roster" + ":jabber.iq.time" + ":jabber.iq.version"
                + ":org.jabber.etherx.streams" + ":org.jabber.protocol.activity" + ":org.jabber.protocol.address"
                + ":org.jabber.protocol.amp" + ":org.jabber.protocol.amp_errors"
                + ":org.jabber.protocol.disco_info" + ":org.jabber.protocol.disco_items"
                + ":org.jabber.protocol.muc" + ":org.jabber.protocol.muc_admin"
                + ":org.jabber.protocol.muc_unique" + ":org.jabber.protocol.muc_user"
                + ":ietf.params.xml.ns.xmpp_sasl" + ":ietf.params.xml.ns.xmpp_stanzas"
                + ":ietf.params.xml.ns.xmpp_streams" + ":ietf.params.xml.ns.xmpp_tls");

        inputStream = new TcpBufferedInputStream(socket.getInputStream(), 8 * 1024);
        outputStream = new TcpBufferedOutputStream(socket.getOutputStream(), 16 * 1024);

        unmarshaller = context.createUnmarshaller();
        marshaller = context.createMarshaller();
        marshaller.setProperty(Marshaller.JAXB_FRAGMENT, true);
    }

    protected void writeOpenStream(String id, String from) throws IOException, XMLStreamException {
        XMLOutputFactory factory = XMLOutputFactory.newInstance();
        //factory.setProperty(XMLOutputFactory.IS_REPAIRING_NAMESPACES, true);
        xmlWriter = factory.createXMLStreamWriter(outputStream);

        // write the dummy start tag
        xmlWriter.writeStartDocument();
        xmlWriter.writeStartElement("stream", "stream", "http://etherx.jabber.org/streams");
        xmlWriter.writeDefaultNamespace("jabber:client");
        xmlWriter.writeNamespace("stream", "http://etherx.jabber.org/streams");
        xmlWriter.writeAttribute("version", "1.0");
        xmlWriter.writeAttribute("id", id);
        if (to == null) {
            to = "client";
        }
        xmlWriter.writeAttribute("to", to);
        xmlWriter.writeAttribute("from", from);
        xmlWriter.writeCharacters("\n");
    }

}
