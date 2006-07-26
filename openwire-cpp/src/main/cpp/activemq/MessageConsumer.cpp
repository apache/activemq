/*
 * Copyright 2006 The Apache Software Foundation or its licensors, as
 * applicable.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "activemq/MessageConsumer.hpp"
#include "activemq/Session.hpp"

using namespace apache::activemq;

/*
 * 
 */
MessageConsumer::MessageConsumer(p<Session> session, p<ConsumerInfo> consumerInfo, AcknowledgementMode acknowledgementMode)
{
    this->session                = session ;
    this->consumerInfo           = consumerInfo ;
    this->acknowledgementMode    = acknowledgementMode ;
    this->dispatcher             = new Dispatcher() ;
    this->listener               = NULL ;
    this->closed                 = false ;
    this->maximumRedeliveryCount = 10 ;
    this->redeliveryTimeout      = 500 ;
}

/*
 *
 */
MessageConsumer::~MessageConsumer()
{
    // Make sure consumer is closed
    close() ;
}

// Attribute methods ------------------------------------------------

/*
 *
 */
void MessageConsumer::setMessageListener(p<IMessageListener> listener)
{
    this->listener = listener ;
}

/*
 *
 */
p<IMessageListener> MessageConsumer::getMessageListener()
{
    return listener ;
}

/*
 *
 */
p<ConsumerId> MessageConsumer::getConsumerId()
{
    return consumerInfo->getConsumerId() ;
}

/*
 *
 */
void MessageConsumer::setMaximumRedeliveryCount(int count)
{
    this->maximumRedeliveryCount = count ;
}

/*
 *
 */
int MessageConsumer::getMaximumRedeliveryCount()
{
    return maximumRedeliveryCount ;
}

/*
 *
 */
void MessageConsumer::setRedeliveryTimeout(int timeout)
{
    this->redeliveryTimeout = timeout ;
}

/*
 *
 */
int MessageConsumer::getRedeliveryTimeout()
{
    return redeliveryTimeout ;
}


// Operation methods ------------------------------------------------

/*
 *
 */
p<IMessage> MessageConsumer::receive()
{
    checkClosed() ;
    return autoAcknowledge( dispatcher->dequeue() ) ;
}

/*
 *
 */
p<IMessage> MessageConsumer::receive(int timeout)
{
    checkClosed() ;
    return autoAcknowledge( dispatcher->dequeue(timeout) ) ;
}

/*
 *
 */
p<IMessage> MessageConsumer::receiveNoWait()
{
    checkClosed() ;
    return autoAcknowledge( dispatcher->dequeueNoWait() ) ;
}

/*
 *
 */
void MessageConsumer::redeliverRolledBackMessages()
{
    dispatcher->redeliverRolledBackMessages() ;
}

/*
 * Transport callback that handles messages dispatching
 */
void MessageConsumer::dispatch(p<IMessage> message)
{
    dispatcher->enqueue(message) ;

    // Activate background dispatch thread if async listener is set up
    if( listener != NULL )
        session->dispatch() ;
}

/*
 *
 */
void MessageConsumer::dispatchAsyncMessages()
{
    while( listener != NULL )
    {
        p<IMessage> message = dispatcher->dequeueNoWait() ;

        if( message != NULL )
        {
            // Auto acknowledge message if selected
            autoAcknowledge(message) ;

            // Let listener process message
            listener->onMessage(message) ;
        }
        else
            break ;
    }
}

/*
 * IAcknowledger callback method.
 */
void MessageConsumer::acknowledge(p<ActiveMQMessage> message)
{
    doClientAcknowledge(message) ;
}

/*
 * 
 */
void MessageConsumer::close()
{
    if( !closed )
    {
        closed = true ;
    
        // De-register consumer from broker
        session->getConnection()->disposeOf( consumerInfo->getConsumerId() ) ;

        // Reset internal state (prevent cyclic references)
        session = NULL ;
    }
}


// Implementation methods ------------------------------------------------

/*
 *
 */
void MessageConsumer::checkClosed() throw(CmsException) 
{
    if( closed )
        throw ConnectionClosedException("Oops! Connection already closed") ;
}

/*
 *
 */
p<IMessage> MessageConsumer::autoAcknowledge(p<IMessage> message)
{
    try
    {
        if( message != NULL )
        {
            // Is the message an ActiveMQMessage? (throws bad_cast otherwise)
            p<ActiveMQMessage> activeMessage = 
                p_dyncast<ActiveMQMessage> (message);
    
            // Register the handler for client acknowledgment
            activeMessage->setAcknowledger( smartify(this) );

            if( acknowledgementMode != ClientAckMode )
                doAcknowledge(activeMessage);
        }         
    }
    catch( bad_cast& bc )
    {
        // ignore
    }

    // Return the message even if NULL, caller must determine what to do.
    return message;
}

/*
 *
 */
void MessageConsumer::doClientAcknowledge(p<ActiveMQMessage> message)
{
    if( acknowledgementMode == ClientAckMode )
        doAcknowledge(message);
}

/*
 *
 */
void MessageConsumer::doAcknowledge(p<Message> message)
{
    p<MessageAck> ack = createMessageAck(message) ;
    session->getConnection()->oneway(ack) ;
}

/*
 *
 */
p<MessageAck> MessageConsumer::createMessageAck(p<Message> message)
{
    p<MessageAck> ack = new MessageAck() ;

    // Set ack properties
    ack->setAckType( ConsumedAck ) ;
    ack->setConsumerId( consumerInfo->getConsumerId() ) ;
    ack->setDestination( message->getDestination() ) ;
    ack->setFirstMessageId( message->getMessageId() ) ;
    ack->setLastMessageId( message->getMessageId() ) ;
    ack->setMessageCount( 1 ) ;
    
    if( session->isTransacted() )
    {
        session->doStartTransaction() ;
        ack->setTransactionId( session->getTransactionContext()->getTransactionId() ) ;
        session->getTransactionContext()->addSynchronization( new MessageConsumerSynchronization(smartify(this), message) ) ;
    }
    return ack ;
}

/*
 *
 */
void MessageConsumer::afterRollback(p<ActiveMQMessage> message)
{
    // Try redeliver of the message again
    message->setRedeliveryCounter( message->getRedeliveryCounter() + 1 ) ;

    // Check if redeliver count has exceeded maximum
    if( message->getRedeliveryCounter() > maximumRedeliveryCount )
    {
        // Send back a poisoned pill
        p<MessageAck> ack = new MessageAck() ;
        ack->setAckType( PoisonAck ) ;
        ack->setConsumerId( consumerInfo->getConsumerId() ) ;
        ack->setDestination( message->getDestination() ) ;
        ack->setFirstMessageId( message->getMessageId() ) ;
        ack->setLastMessageId( message->getMessageId() ) ;
        ack->setMessageCount( 1 ) ;
        session->getConnection()->oneway(ack) ;
    }
    else
    {
        dispatcher->redeliver(message) ;
        
        // Re-dispatch the message at some point in the future
        if( listener != NULL )
            session->dispatch( redeliveryTimeout ) ;
    }
}
