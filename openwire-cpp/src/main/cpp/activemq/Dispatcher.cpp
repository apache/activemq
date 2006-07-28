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
#include "activemq/Dispatcher.hpp"

using namespace apache::activemq;

/*
 * 
 */
Dispatcher::Dispatcher()
{
    dispatchQueue = new queue< p<IMessage> > ;
    redeliverList = new list< p<IMessage> > ;
}

/*
 * 
 */
void Dispatcher::redeliverRolledBackMessages()
{
    LOCKED_SCOPE (mutex);

    p<queue< p<IMessage> > > replacementQueue = new queue< p<IMessage> > ;
    //(dispatchQueue->size() + redeliverList->size() ) ;

    // Copy all messages to be redelivered to the new queue
    while( !redeliverList->empty() )
    {
        replacementQueue->push( redeliverList->front() ) ;
        redeliverList->pop_front() ;
    }

    // Copy all messages to be dispatched to the new queue
    while( dispatchQueue->size() > 0 )
    {
        // Get first element in queue
        p<IMessage> element = p_cast<IMessage> (dispatchQueue->front()) ;

        // Remove first element from queue
        dispatchQueue->pop() ;

        // Add element to the new queue
        replacementQueue->push(element) ;
    }
    // Switch to the new queue
    dispatchQueue = replacementQueue ;

    semaphore.notify() ;
}

/*
 * 
 */
void Dispatcher::redeliver(p<IMessage> message)
{
    LOCKED_SCOPE (mutex);
    redeliverList->push_back(message) ;
}

/*
 * 
 */
void Dispatcher::enqueue(p<IMessage> message)
{
    LOCKED_SCOPE (mutex);
    dispatchQueue->push(message) ;
    semaphore.notify() ;
}

/*
 * 
 */
p<IMessage> Dispatcher::dequeueNoWait()
{
    p<IMessage> msg = NULL ;

    {
        LOCKED_SCOPE (mutex);

        if( dispatchQueue->size() > 0 )
        {
            msg = p_cast<IMessage> (dispatchQueue->front()) ;
            dispatchQueue->pop() ;
        }
    }        
    return msg ;
}

/*
 * 
 */
p<IMessage> Dispatcher::dequeue(int timeout)
{
    p<IMessage> msg = NULL ;

    {
        LOCKED_SCOPE (mutex);

        if( dispatchQueue->size() == 0 )
            semaphore.wait(timeout) ;

        if( dispatchQueue->size() > 0 )
        {
            msg = p_cast<IMessage> (dispatchQueue->front()) ;
            dispatchQueue->pop() ;
        }
    }
    return msg ;
}

/*
 * 
 */
p<IMessage> Dispatcher::dequeue()
{
    p<IMessage> msg = NULL ;

    {
        LOCKED_SCOPE (mutex);

        msg = p_cast<IMessage> (dispatchQueue->front()) ;
        dispatchQueue->pop() ;
    }

    return msg ;
}
