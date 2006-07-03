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
#ifndef _ACTIVEMQ_CORE_ACTIVEMQCONSUMER_H_
#define _ACTIVEMQ_CORE_ACTIVEMQCONSUMER_H_

#include <cms/MessageConsumer.h>
#include <cms/MessageListener.h>
#include <cms/Message.h>
#include <cms/CMSException.h>

#include <activemq/connector/ConsumerInfo.h>
#include <activemq/util/Queue.h>
#include <activemq/core/ActiveMQAckHandler.h>
#include <activemq/core/ActiveMQMessageListener.h>
#include <activemq/core/ActiveMQSessionResource.h>
#include <activemq/concurrent/Runnable.h>
#include <activemq/concurrent/Mutex.h>

namespace activemq{
namespace core{

    class ActiveMQSession;

    class ActiveMQConsumer : 
        public cms::MessageConsumer,
        public ActiveMQAckHandler,
        public concurrent::Runnable,
        public ActiveMQMessageListener,
        public ActiveMQSessionResource
    {
    private:
    
        // The session that owns this Consumer
        ActiveMQSession* session;
        
        // The Consumer info for this Consumer
        connector::ConsumerInfo* consumerInfo;
        
        // The Message Listener for this Consumer
        cms::MessageListener* listener;
        
        // Lock to protect us from dispatching to a dead listener
        concurrent::Mutex listenerLock;
        
        // Message Queue
        util::Queue<cms::Message*> msgQueue;
        
        // Thread to notif a listener if one is added
        concurrent::Thread* listenerThread;
        
        // Boolean to indicate that the listener Thread is shutting
        // down and the run method should return.
        bool shutdown;
        
    public:

        /**
         * Constructor
         */
        ActiveMQConsumer(connector::ConsumerInfo* consumerInfo,
                         ActiveMQSession* session);

        /**
         * Destructor
         */
        virtual ~ActiveMQConsumer(void);

    public:  // Interface Implementation
    
        /**
         * Synchronously Receive a Message
         * @return new message
         * @throws CMSException
         */
        virtual cms::Message* receive(void) throw ( cms::CMSException );

        /**
         * Synchronously Receive a Message, time out after defined interval.
         * Returns null if nothing read.
         * @return new message
         * @throws CMSException
         */
        virtual cms::Message* receive(int millisecs) throw ( cms::CMSException );

        /**
         * Receive a Message, does not wait if there isn't a new message
         * to read, returns NULL if nothing read.
         * @return new message
         * @throws CMSException
         */
        virtual cms::Message* receiveNoWait(void) throw ( cms::CMSException );

        /**
         * Sets the MessageListener that this class will send notifs on
         * @param MessageListener interface pointer
         */
        virtual void setMessageListener(cms::MessageListener* listener);

        /**
         * Gets the MessageListener that this class will send notifs on
         * @param MessageListener interface pointer
         */
        virtual cms::MessageListener* getMessageListener(void) const {
            return this->listener;
        }

        /**
         * Gets this message consumer's message selector expression.
         * @return This Consumer's selector expression or "".
         * @throws cms::CMSException
         */
        virtual std::string getMessageSelector(void) const 
          throw ( cms::CMSException );
          
        /**
         * Method called to acknowledge the message passed
         * @param Message to Acknowlegde
         * @throw CMSException
         */
        virtual void acknowledgeMessage( const ActiveMQMessage* message )
            throw ( cms::CMSException );

        /**
         * Run method that is called from the Thread class when this object
         * is registered with a Thread and started.  This function reads from
         * the message queue and dispatches calls to the MessageConsumer that
         * is registered with this class.
         * 
         * It is a error for a MessageListener to throw an exception in their
         * onMessage method, but if it does happen this function will get any
         * registered exception listener from the session and notify it.
         */            
        virtual void run(void);

    public:  // ActiveMQMessageListener Methods
    
        /**
         * Called asynchronously when a new message is received, the message
         * that is passed is now the property of the callee, and the caller
         * will disavowe all knowledge of the message, i.e Callee must delete.
         * @param Message object pointer
         */
        virtual void onActiveMQMessage( ActiveMQMessage* message ) 
            throw ( exceptions::ActiveMQException );
    
    public:  // ActiveMQSessionResource
    
        /**
         * Retrieve the Connector resource that is associated with
         * this Session resource.
         * @return pointer to a Connector Resource, can be NULL
         */
        virtual connector::ConnectorResource* getConnectorResource(void) {
            return consumerInfo;
        }

    public:  // ActiveMQConsumer Methods
    
        /**
         * Called to dispatch a message to this consumer, this is usually
         * called from the context of another thread.  This will enqueue a
         * message on the Consumers Queue, or notify a listener if one is
         * currently registered.
         * @param cms::Message pointer to the message to dispatch
         * @throw cms::CMSException
         */
        virtual void dispatch(ActiveMQMessage* message) 
            throw ( cms::CMSException );

        /**
         * Get the Consumer information for this consumer
         * @return Pointer to a Consumer Info Object            
         */
        virtual connector::ConsumerInfo* getConsumerInfo(void) {
            return consumerInfo;
        }

    protected:
            
        /**
         * Purges all messages currently in the queue.  This can be as a
         * result of a rollback, or of the consumer being shutdown.
         */
        virtual void purgeMessages(void);
        
        /**
         * Destroys the message if the session is transacted, otherwise
         * does nothing.
         */
        virtual void destroyMessage( cms::Message* message );

        /**
         * Notifies the listener of a message.
         */
        void notifyListener( cms::Message* message );
        
        /**
         * Starts the message processing thread to receive messages
         * asynchronously.  This thread is started when setMessageListener
         * is invoked, which means that the caller is choosing to use this
         * consumer asynchronously instead of synchronously (receive).
         */
        void startThread();
        
        /**
         * Stops the asynchronous message processing thread if it's started.
         */
        void stopThread();
    };

}}

#endif /*_ACTIVEMQ_CORE_ACTIVEMQCONSUMER_H_*/
