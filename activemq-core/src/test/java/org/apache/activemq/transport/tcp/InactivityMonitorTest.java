package org.apache.activemq.transport.tcp;

import java.io.IOException;
import java.net.URI;

import org.apache.activemq.CombinationTestSupport;
import org.apache.activemq.command.Command;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportAcceptListener;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.transport.TransportListener;
import org.apache.activemq.transport.TransportServer;

import edu.emory.mathcs.backport.java.util.concurrent.atomic.AtomicBoolean;
import edu.emory.mathcs.backport.java.util.concurrent.atomic.AtomicInteger;


public class InactivityMonitorTest extends CombinationTestSupport implements TransportAcceptListener {
    
    private TransportServer server;
    private Transport clientTransport;
    private Transport serverTransport;
    
    private final AtomicInteger clientReceiveCount = new AtomicInteger(0);
    private final AtomicInteger clientErrorCount = new AtomicInteger(0);
    private final AtomicInteger serverReceiveCount = new AtomicInteger(0);
    private final AtomicInteger serverErrorCount = new AtomicInteger(0);
    
    private final AtomicBoolean ignoreClientError = new AtomicBoolean(false);
    private final AtomicBoolean ignoreServerError = new AtomicBoolean(false);
    
    public Runnable serverRunOnCommand;
    public Runnable clientRunOnCommand;
    
    public long clientInactivityLimit;
    public long serverInactivityLimit;

    
    protected void setUp() throws Exception {
        super.setUp();
        server = TransportFactory.bind("localhost", new URI("tcp://localhost:61616?maxInactivityDuration="+serverInactivityLimit));
        server.setAcceptListener(this);
        server.start();
        clientTransport = TransportFactory.connect(new URI("tcp://localhost:61616?maxInactivityDuration="+clientInactivityLimit));
        clientTransport.setTransportListener(new TransportListener() {
            public void onCommand(Command command) {
                clientReceiveCount.incrementAndGet();
                if( clientRunOnCommand !=null ) {
                    clientRunOnCommand.run();
                }
            }
            public void onException(IOException error) {
                if( !ignoreClientError.get() ) {
                    System.out.println("Client transport error:");
                    error.printStackTrace();
                    clientErrorCount.incrementAndGet();
                }
            }
            public void transportInterupted() {
            }
            public void transportResumed() {
            }});
        clientTransport.start();
    }
    
    protected void tearDown() throws Exception {
        ignoreClientError.set(true);
        ignoreServerError.set(true);
        clientTransport.stop();
        serverTransport.stop();
        server.stop();
        super.tearDown();
    }
    
    public void onAccept(Transport transport) {
        try {
            serverTransport = transport;
            serverTransport.setTransportListener(new TransportListener() {
                public void onCommand(Command command) {
                    serverReceiveCount.incrementAndGet();
                    if( serverRunOnCommand !=null ) {
                        serverRunOnCommand.run();
                    }
                }
                public void onException(IOException error) {
                    if( !ignoreClientError.get() ) {
                        System.out.println("Server transport error:");
                        error.printStackTrace();
                        serverErrorCount.incrementAndGet();
                    }
                }
                public void transportInterupted() {
                }
                public void transportResumed() {
                }});
            serverTransport.start();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void onAcceptError(Exception error) {
        error.printStackTrace();
    }

    public void initCombosForTestClientHang() {
        addCombinationValues("clientInactivityLimit", new Object[] { new Long(1000*60)});
        addCombinationValues("serverInactivityLimit", new Object[] { new Long(1000)});
    }
    public void testClientHang() throws Exception {
        
        assertEquals(0, serverErrorCount.get());
        assertEquals(0, clientErrorCount.get());
        
        // Server should consider the client timed out right away since the client is not hart beating fast enough.
        Thread.sleep(3000);
        
        assertEquals(0, clientErrorCount.get());
        assertTrue(serverErrorCount.get()>0);
    }
    
    public void initCombosForTestNoClientHang() {
        addCombinationValues("clientInactivityLimit", new Object[] { new Long(1000)});
        addCombinationValues("serverInactivityLimit", new Object[] { new Long(1000)});
    }
    public void testNoClientHang() throws Exception {
        
        assertEquals(0, serverErrorCount.get());
        assertEquals(0, clientErrorCount.get());
        
        Thread.sleep(4000);
        
        assertEquals(0, clientErrorCount.get());
        assertEquals(0, serverErrorCount.get());
    }

    /**
     * Used to test when a operation blocks.  This should
     * not cause transport to get disconnected.
     */
    public void initCombosForTestNoClientHangWithServerBlock() {
        addCombinationValues("clientInactivityLimit", new Object[] { new Long(1000)});
        addCombinationValues("serverInactivityLimit", new Object[] { new Long(1000)});
        addCombinationValues("serverRunOnCommand", new Object[] { new Runnable() {
                public void run() {
                    try {
                        System.out.println("Sleeping");
                        Thread.sleep(2000);
                    } catch (InterruptedException e) {
                    }
                }
            }});
    }
    public void testNoClientHangWithServerBlock() throws Exception {
        
        assertEquals(0, serverErrorCount.get());
        assertEquals(0, clientErrorCount.get());
        
        Thread.sleep(4000);
        
        assertEquals(0, clientErrorCount.get());
        assertEquals(0, serverErrorCount.get());
    }

}
