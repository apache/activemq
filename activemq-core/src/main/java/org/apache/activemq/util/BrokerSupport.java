package org.apache.activemq.util;

import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.Message;

public class BrokerSupport {
    
    /**
     * @param context
     * @param message
     * @param deadLetterDestination
     * @throws Exception
     */
    static public void resend(final ConnectionContext context, Message message, ActiveMQDestination deadLetterDestination) throws Exception {
        if(message.getOriginalDestination()!=null)
            message.setOriginalDestination(message.getDestination());
        if(message.getOriginalTransactionId()!=null)
            message.setOriginalTransactionId(message.getTransactionId());                            
        message.setDestination(deadLetterDestination);
        message.setTransactionId(null);
        message.evictMarshlledForm();
        boolean originalFlowControl=context.isProducerFlowControl();
        try{
            context.setProducerFlowControl(false);
            context.getBroker().send(context,message);
        }finally{
            context.setProducerFlowControl(originalFlowControl);
        }
    }

}
