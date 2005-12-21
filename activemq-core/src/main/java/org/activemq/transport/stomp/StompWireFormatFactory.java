/*
 * Copyright (c) 2005 Your Corporation. All Rights Reserved.
 */
package org.activemq.transport.stomp;

import org.activeio.command.WireFormat;
import org.activeio.command.WireFormatFactory;

/**
 * Creates WireFormat objects that implement the <a href="http://stomp.codehaus.org/">Stomp</a> protocol.
 */
public class StompWireFormatFactory implements WireFormatFactory {
    public WireFormat createWireFormat() {
        return new StompWireFormat();
    }
}
