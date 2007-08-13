package org.apache.activemq.transport.logwriters;

import java.io.IOException;

import org.apache.activemq.transport.LogWriter;
import org.apache.commons.logging.Log;

/**
 * Implementation of LogWriter interface to keep ActiveMQ's
 * old logging format.
 */
public class DefaultLogWriter implements LogWriter {

    // doc comment inherited from LogWriter
    public void initialMessage(Log log) {
        // Default log writer does nothing here
    }

    // doc comment inherited from LogWriter
    public void logRequest (Log log, Object command) {
        log.debug("SENDING REQUEST: "+command);
    }

    // doc comment inherited from LogWriter
    public void logResponse (Log log, Object response) {
        log.debug("GOT RESPONSE: "+response);
    }

    // doc comment inherited from LogWriter
    public void logAsyncRequest (Log log, Object command) {
        log.debug("SENDING ASNYC REQUEST: "+command);
    }

    // doc comment inherited from LogWriter
    public void logOneWay (Log log, Object command) {
        log.debug("SENDING: "+command);
    }

    // doc comment inherited from LogWriter
    public void logReceivedCommand (Log log, Object command) {
        log.debug("RECEIVED: " + command);
    }

    // doc comment inherited from LogWriter
    public void logReceivedException (Log log, IOException error) {
        log.debug("RECEIVED Exception: "+error, error);
    }


}
