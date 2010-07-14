/*
 * Copyright (c) 2007, 2008 XStream Committers.
 * All rights reserved.
 *
 * The software in this package is published under the terms of the BSD
 * style license a copy of which has been included with this distribution in
 * the LICENSE.txt file.
 * 
 * Created on 30. March 2007 by Joerg Schaible
 */
package org.apache.activemq.util;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;

import javax.xml.stream.XMLStreamException;

import org.codehaus.jettison.mapped.Configuration;
import org.codehaus.jettison.mapped.MappedNamespaceConvention;
import org.codehaus.jettison.mapped.MappedXMLInputFactory;
import org.codehaus.jettison.mapped.MappedXMLOutputFactory;

import com.thoughtworks.xstream.io.HierarchicalStreamDriver;
import com.thoughtworks.xstream.io.HierarchicalStreamReader;
import com.thoughtworks.xstream.io.HierarchicalStreamWriter;
import com.thoughtworks.xstream.io.StreamException;
import com.thoughtworks.xstream.io.json.JettisonStaxWriter;
import com.thoughtworks.xstream.io.xml.QNameMap;
import com.thoughtworks.xstream.io.xml.StaxReader;
import com.thoughtworks.xstream.io.xml.StaxWriter;


/**
 * 
 * Temporary used until XStream 1.3.2 is released
 * 
 * Simple XStream driver wrapping Jettison's Mapped reader and writer. Serializes object from
 * and to JSON.
 * 
 * @author Dejan Bosanac
 */
public class JettisonMappedXmlDriver implements HierarchicalStreamDriver {

    private final MappedXMLOutputFactory mof;
    private final MappedXMLInputFactory mif;
    private final MappedNamespaceConvention convention;
    private boolean useSerializeAsArray = true;

    public JettisonMappedXmlDriver() {
        this(new Configuration(), true);
    }

    public JettisonMappedXmlDriver(final Configuration config, final boolean useSerializeAsArray) {
        mof = new MappedXMLOutputFactory(config);
        mif = new MappedXMLInputFactory(config);
        convention = new MappedNamespaceConvention(config);
        this.useSerializeAsArray = useSerializeAsArray;
    }
    
    public HierarchicalStreamReader createReader(final Reader reader) {
        try {
            return new StaxReader(new QNameMap(), mif.createXMLStreamReader(reader));
        } catch (final XMLStreamException e) {
            throw new StreamException(e);
        }
    }

    public HierarchicalStreamReader createReader(final InputStream input) {
        try {
            return new StaxReader(new QNameMap(), mif.createXMLStreamReader(input));
        } catch (final XMLStreamException e) {
            throw new StreamException(e);
        }
    }

    public HierarchicalStreamWriter createWriter(final Writer writer) {
        try {
            if (useSerializeAsArray) {
                return new JettisonStaxWriter(new QNameMap(), mof.createXMLStreamWriter(writer), convention);
            } else {
                return new StaxWriter(new QNameMap(), mof.createXMLStreamWriter(writer));
            }
        } catch (final XMLStreamException e) {
            throw new StreamException(e);
        }
    }

    public HierarchicalStreamWriter createWriter(final OutputStream output) {
        try {
            if (useSerializeAsArray) {
                return new JettisonStaxWriter(new QNameMap(), mof.createXMLStreamWriter(output), convention);
            } else {
                return new StaxWriter(new QNameMap(), mof.createXMLStreamWriter(output));
            }
        } catch (final XMLStreamException e) {
            throw new StreamException(e);
        }
    }

}