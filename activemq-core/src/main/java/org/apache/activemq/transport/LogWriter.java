package org.apache.activemq.transport;

import java.io.IOException;

import org.apache.commons.logging.Log;

/**
 * Interface for classes that will be called by the TransportLogger
 * class to actually write to a log file.
 * Every class that implements this interface has do be declared in
 * the resources/META-INF/services/org/apache/activemq/transport/logwriters
 * directory, by creating a file with the name of the writer (for example
 * "default") and including the line
 * class=org.apache.activemq.transport.logwriters.(Name of the LogWriter class)
 */
public interface LogWriter {

    /**
     * Writes a header message to the log.
     * @param log The log to be written to.
     */
    public void initialMessage(Log log);
    
    /**
     * Writes a message to a log when a request command is sent.
     * @param log The log to be written to.
     * @param command The command to be logged.
     */
    public void logRequest (Log log, Object command);
    
    /**
     * Writes a message to a log when a response command is received.
     * @param log The log to be written to.
     * @param command The command to be logged.
     */
    public void logResponse (Log log, Object response);

    /**
     * Writes a message to a log when an asynchronous equest command is sent.
     * @param log The log to be written to.
     * @param command The command to be logged.
     */
    public void logAsyncRequest (Log log, Object command);
    
    /**
     * Writes a message to a log when message is sent.
     * @param log The log to be written to.
     * @param command The command to be logged.
     */
    public void logOneWay (Log log, Object command);
    
    /**
     * Writes a message to a log when message is received.
     * @param log The log to be written to.
     * @param command The command to be logged.
     */
    public void logReceivedCommand (Log log, Object command);
    
    /**
     * Writes a message to a log when an exception is received.
     * @param log The log to be written to.
     * @param command The command to be logged.
     */
    public void logReceivedException (Log log, IOException error);
    
}
