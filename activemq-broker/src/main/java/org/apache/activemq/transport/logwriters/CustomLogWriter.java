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
package org.apache.activemq.transport.logwriters;

import java.io.IOException;

import org.apache.activemq.command.BaseCommand;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageDispatch;
import org.apache.activemq.command.ProducerAck;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.command.WireFormatInfo;
import org.apache.activemq.transport.LogWriter;
import org.slf4j.Logger;

/**
 * Custom implementation of LogWriter interface.
 * 
 * @author David Martin Clavo david(dot)martin(dot)clavo(at)gmail.com
 * 
 */
public class CustomLogWriter implements LogWriter {
    
    // doc comment inherited from LogWriter
    public void initialMessage(Logger log) {
        
    }

    // doc comment inherited from LogWriter
    public void logRequest (Logger log, Object command) {
        log.debug("$$ SENDREQ: " + CustomLogWriter.commandToString(command));
    }
    
    // doc comment inherited from LogWriter
    public void logResponse (Logger log, Object response) {
        log.debug("$$ GOT_RESPONSE: "+response);
    }
    
    // doc comment inherited from LogWriter
    public void logAsyncRequest (Logger log, Object command) {
        log.debug("$$ SENDING_ASNYC_REQUEST: "+command);
    }
    
    // doc comment inherited from LogWriter
    public void logOneWay (Logger log, Object command) {
        log.debug("$$ SENDING: " + CustomLogWriter.commandToString(command));
    }
    
    // doc comment inherited from LogWriter
    public void logReceivedCommand (Logger log, Object command) {
        log.debug("$$ RECEIVED: " + CustomLogWriter.commandToString(command));
    }
    
    // doc comment inherited from LogWriter
    public void logReceivedException (Logger log, IOException error) {
        log.debug("$$ RECEIVED_EXCEPTION: "+error, error);
    }
    
    /**
     * Transforms a command into a String
     * @param command An object (hopefully of the BaseCommand class or subclass)
     * to be transformed into String.
     * @return A String which will be written by the CustomLogWriter.
     * If the object is not a BaseCommand, the String 
     * "Unrecognized_object " + command.toString()
     * will be returned.
     */
    private static String commandToString(Object command) {
        StringBuilder sb = new StringBuilder();
        
        if (command instanceof BaseCommand) {

            BaseCommand bc = (BaseCommand)command;
            sb.append(command.getClass().getSimpleName());
            sb.append(' ');
            sb.append(bc.isResponseRequired() ? 'T' : 'F');
            
            
            Message m = null;
            
            if (bc instanceof Message) {
                m = (Message)bc;
            }
            if (bc instanceof MessageDispatch){
                m = ((MessageDispatch)bc).getMessage();   
            }
                
            if (m != null) {
                sb.append(' ');
                sb.append(m.getMessageId());
                sb.append(',');
                sb.append(m.getCommandId());
                ProducerId pid = m.getProducerId();
                long sid = pid.getSessionId();
                sb.append(',');
                sb.append(pid.getConnectionId());
                sb.append(',');
                sb.append(sid);
                sb.append(',');
                sb.append(pid.getValue());
                sb.append(',');
                sb.append(m.getCorrelationId());
                sb.append(',');
                sb.append(m.getType());
            }
            
            if (bc instanceof MessageDispatch){
                sb.append(" toConsumer:");
                sb.append(((MessageDispatch)bc).getConsumerId());
            }
            
            if (bc instanceof ProducerAck) {
                sb.append(" ProducerId:");
                sb.append(((ProducerAck)bc).getProducerId());
            }
            
            if (bc instanceof MessageAck) {
                MessageAck ma = (MessageAck)bc;
                sb.append(" ConsumerID:");
                sb.append(ma.getConsumerId());
                sb.append(" ack:");
                sb.append(ma.getFirstMessageId());
                sb.append('-');
                sb.append(ma.getLastMessageId());
            }
            
            if (bc instanceof ConnectionInfo) {
                ConnectionInfo ci = (ConnectionInfo)bc;
                
                sb.append(' ');
                sb.append(ci.getConnectionId());
            }
            
        } else if (command instanceof WireFormatInfo){
            sb.append("WireFormatInfo");
            
        } else {
            sb.append("Unrecognized_object ");
            sb.append(command.toString());
        }
        
        return sb.toString();
    }

}
