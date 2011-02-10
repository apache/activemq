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
package org.apache.activemq.transport.xmpp;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.URI;

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

import ietf.params.xml.ns.xmpp_sasl.Mechanisms;

import org.apache.activemq.command.BrokerInfo;
import org.apache.activemq.command.Command;
import org.apache.activemq.transport.tcp.TcpBufferedInputStream;
import org.apache.activemq.transport.tcp.TcpBufferedOutputStream;
import org.apache.activemq.transport.tcp.TcpTransport;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.ServiceStopper;
import org.apache.activemq.wireformat.WireFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.jabber.etherx.streams.Features;

/**
 * @version $Revision$
 */
public class XmppTransport extends TcpTransport {
    protected static final QName ATTRIBUTE_TO = new QName("to");

    private static final transient Logger LOG = LoggerFactory.getLogger(XmppTransport.class);

    protected OutputStream outputStream;
    protected InputStream inputStream;

    private static JAXBContext context;
    private XMLEventReader xmlReader;
    private Unmarshaller unmarshaller;
    private Marshaller marshaller;
    private XMLStreamWriter xmlWriter;
    private String to = "client";
    private ProtocolConverter converter;
    private String from = "localhost";
    private String brokerId = "broker-id-1";

    public XmppTransport(WireFormat wireFormat, Socket socket) throws IOException {
        super(wireFormat, socket);
        init();
    }

    public XmppTransport(WireFormat wireFormat, SocketFactory socketFactory, URI uri, URI uri1) throws IOException {
        super(wireFormat, socketFactory, uri, uri1);
        init();
    }

    private void init() {
        LOG.debug("Creating new instance of XmppTransport");
        converter = new ProtocolConverter(this);
    }

    @Override
    public void oneway(Object object) throws IOException {
        if (object instanceof Command) {
            Command command = (Command)object;

            if (command instanceof BrokerInfo) {
                BrokerInfo brokerInfo = (BrokerInfo)command;

                brokerId = brokerInfo.getBrokerId().toString();
                from = brokerInfo.getBrokerName();
                try {
                    writeOpenStream(brokerId, from);
                } catch (XMLStreamException e) {
                    throw IOExceptionSupport.create(e);
                }
            } else {
                try {
                    converter.onActiveMQCommand(command);
                } catch (IOException e) {
                    throw e;
                } catch (Exception e) {
                    throw IOExceptionSupport.create(e);
                }
            }
        } else {
            LOG.warn("Unkown command: " + object);
        }
    }

    /**
     * Marshalls the given POJO to the client
     */
    public void marshall(Object command) throws IOException {
        if (isStopped() || isStopping()) {
            LOG.warn("Not marshalling command as shutting down: " + command);
            return;
        }
        try {
            marshaller.marshal(command, xmlWriter);
            xmlWriter.flush();
            outputStream.flush();
        } catch (JAXBException e) {
            throw IOExceptionSupport.create(e);
        } catch (XMLStreamException e) {
            throw IOExceptionSupport.create(e);
        }
    }

    @Override
    public void doRun() throws IOException {
        LOG.debug("XMPP consumer thread starting");
        try {
            XMLInputFactory xif = XMLInputFactory.newInstance();
            xif.setXMLReporter(new XMLReporter() {
                public void report(String message, String errorType, Object relatedInformation, Location location) throws XMLStreamException {
                    LOG.warn(message + " errorType: " + errorType + " relatedInfo: " + relatedInformation);
                }
            });

            xmlReader = xif.createXMLEventReader(inputStream);

            XMLEvent docStart = xmlReader.nextEvent();

            XMLEvent rootElement = xmlReader.nextTag();

            if (rootElement instanceof StartElement) {
                StartElement startElement = (StartElement)rootElement;
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
                        LOG.debug("Unmarshalled new incoming event - " + object.getClass().getName());
                        converter.onXmppCommand(object);
                    }
                } else {
                    if (event.getEventType() == XMLEvent.END_ELEMENT) {
                        break;
                    } else if (event.getEventType() == XMLEvent.END_ELEMENT || event.getEventType() == XMLEvent.END_DOCUMENT) {
                        break;
                    } else {
                        xmlReader.nextEvent();
                    }

                }
            }
        } catch (Exception e) {
            throw IOExceptionSupport.create(e);
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
            } catch (XMLStreamException e) {
                // the client may have closed first so ignore this
                LOG.info("Caught trying to close transport: " + e, e);
            }
        }
        if (xmlReader != null) {
            try {
                xmlReader.close();
            } catch (XMLStreamException e) {
                // the client may have closed first so ignore this
                LOG.info("Caught trying to close transport: " + e, e);
            }
        }
        super.doStop(stopper);
    }

    @Override
    protected void initializeStreams() throws Exception {
        // TODO it would be preferable to use class discovery here!
        if ( context == null ) {
            context = JAXBContext.newInstance(
                    "jabber.server:" +
                    "jabber.server.dialback:" +
                    "jabber.client:" +
                    "jabber.iq._private:" +
                    "jabber.iq.auth:" +
                    "jabber.iq.gateway:" +
                    "jabber.iq.version:" +
                    "jabber.iq.roster:" +
                    "jabber.iq.pass:" +
                    "jabber.iq.last:" +
                    "jabber.iq.oob:" +
                    "jabber.iq.time:" +
                    "storage.rosternotes:" +
                    "ietf.params.xml.ns.xmpp_streams:" +
                    "ietf.params.xml.ns.xmpp_sasl:" +
                    "ietf.params.xml.ns.xmpp_stanzas:" +
                    "ietf.params.xml.ns.xmpp_bind:" +
                    "ietf.params.xml.ns.xmpp_tls:" +
                    "org.jabber.protocol.muc:" +
                    "org.jabber.protocol.rosterx:" +
                    "org.jabber.protocol.disco_info:" +
                    "org.jabber.protocol.disco_items:" +
                    "org.jabber.protocol.activity:" +
                    "org.jabber.protocol.amp_errors:" +
                    "org.jabber.protocol.amp:" +
                    "org.jabber.protocol.address:" +
                    "org.jabber.protocol.muc_user:" +
                    "org.jabber.protocol.muc_admin:" +
                    "org.jabber.etherx.streams");
        }
        inputStream = new TcpBufferedInputStream(socket.getInputStream(), 8 * 1024);
        outputStream = new TcpBufferedOutputStream(socket.getOutputStream(), 16 * 1024);

        unmarshaller = context.createUnmarshaller();
        marshaller = context.createMarshaller();
        marshaller.setProperty(Marshaller.JAXB_FRAGMENT, true);
    }

    protected void writeOpenStream(String id, String from) throws IOException, XMLStreamException {
        LOG.debug("Sending initial stream element");

        XMLOutputFactory factory = XMLOutputFactory.newInstance();
        // factory.setProperty(XMLOutputFactory.IS_REPAIRING_NAMESPACES, true);
        xmlWriter = factory.createXMLStreamWriter(outputStream);
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

        // now lets write the features
        Features features = new Features();

        // TODO support TLS
        // features.getAny().add(new Starttls());

        //Mechanisms mechanisms = new Mechanisms();

        // TODO support SASL
        // mechanisms.getMechanism().add("DIGEST-MD5");
        // mechanisms.getMechanism().add("PLAIN");
        //features.getAny().add(mechanisms);
        features.getAny().add(new ietf.params.xml.ns.xmpp_bind.ObjectFactory().createBind());
        features.getAny().add(new ietf.params.xml.ns.xmpp_session.ObjectFactory().createSession(""));
        marshall(features);

        LOG.debug("Initial stream element sent!");
    }

}
