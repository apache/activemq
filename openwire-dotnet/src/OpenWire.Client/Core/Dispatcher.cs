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
using System.Collections;
using OpenWire.Client.Commands;
using System;

namespace OpenWire.Client.Core
{
    /// <summary>
    /// Handles the multi-threaded dispatching between the transport and the consumers
    /// </summary>
    public class Dispatcher
    {
        Queue queue = Queue.Synchronized( new Queue() );
        
        /// <summary>
        /// Method Enqueue
        /// </summary>
        /// <param name="message">An ActiveMQMessage</param>
        public void Enqueue(ActiveMQMessage message)
        {
            queue.Enqueue(message);
        }
        
        /// <summary>
        /// Method DequeueNoWait
        /// </summary>
        /// <returns>An IMessage</retutns>
        public IMessage DequeueNoWait()
        {
            lock (queue)
            {
                if (queue.Peek() != null)
                {
                    return (IMessage) queue.Dequeue();
                }
            }
            return null;
        }
        
        /// <summary>
        /// Method Dequeue
        /// </summary>
        /// <param name="timeout">A  long</param>
        /// <returns>An IMessage</retutns>
        public IMessage Dequeue(long timeout)
        {
            // TODO
            throw new Exception("Not implemented yet");
        }
        
        /// <summary>
        /// Method Dequeue
        /// </summary>
        /// <returns>An IMessage</retutns>
        public IMessage Dequeue()
        {
            return (IMessage) queue.Dequeue();
        }
        
        
    }
}
