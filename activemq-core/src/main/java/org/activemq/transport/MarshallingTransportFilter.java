package org.activemq.transport;

import java.io.IOException;

import org.activeio.command.WireFormat;
import org.activemq.command.Command;

public class MarshallingTransportFilter extends TransportFilter {

    private final WireFormat localWireFormat;
    private final WireFormat remoteWireFormat;

    public MarshallingTransportFilter(Transport next, WireFormat localWireFormat, WireFormat remoteWireFormat) {
        super(next);
        this.localWireFormat = localWireFormat;
        this.remoteWireFormat = remoteWireFormat;
    }
    
    public void oneway(Command command) throws IOException {
        next.oneway((Command) remoteWireFormat.unmarshal(localWireFormat.marshal(command)));
    }
    
    public void onCommand(Command command) {
        try {
            commandListener.onCommand((Command)localWireFormat.unmarshal(remoteWireFormat.marshal(command)));
        } catch (IOException e) {
            commandListener.onException(e);
        }
    }
    
}
