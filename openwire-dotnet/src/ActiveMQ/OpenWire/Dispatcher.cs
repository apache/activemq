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
using System;
using System.Threading;

using ActiveMQ.OpenWire.Commands;

namespace ActiveMQ.OpenWire
{
    /// <summary>
    /// Handles the multi-threaded dispatching between the transport and the consumers
    /// </summary>
    public class Dispatcher
    {
        Queue queue = new Queue();
        Object semaphore = new Object();
        ArrayList messagesToRedeliver = new ArrayList();

        /// <summary>
        /// Whem we start a transaction we must redeliver any rolled back messages
        /// </summary>
        public void RedeliverRolledBackMessages() {
            lock (semaphore)
            {
                Queue replacement = new Queue(queue.Count + messagesToRedeliver.Count);
                foreach (ActiveMQMessage element in messagesToRedeliver) {
                    replacement.Enqueue(element);
                }
                messagesToRedeliver.Clear();
                
                while (queue.Count > 0)
                {
                    ActiveMQMessage element = (ActiveMQMessage) queue.Dequeue();
                    replacement.Enqueue(element);
                }
                queue = replacement;
                Monitor.PulseAll(semaphore);
            }
        }
        
        /// <summary>
        /// Redeliver the given message, putting it at the head of the queue
        /// </summary>
        public void Redeliver(ActiveMQMessage message)
        {
            lock (semaphore) {
            messagesToRedeliver.Add(message);
            }
        }
        
        /// <summary>
        /// Method Enqueue
        /// </summary>
        public void Enqueue(ActiveMQMessage message)
        {
            lock (semaphore)
            {
                queue.Enqueue(message);
                Monitor.PulseAll(semaphore);
            }
        }
        
        /// <summary>
        /// Method DequeueNoWait
        /// </summary>
        public IMessage DequeueNoWait()
        {
            lock (semaphore)
            {
                if (queue.Count > 0)
                {
                    return (IMessage) queue.Dequeue();
                }
            }
            return null;
        }
        
        /// <summary>
        /// Method Dequeue
        /// </summary>
        public IMessage Dequeue(int timeout)
        {
            lock (semaphore)
            {
                if (queue.Count == 0)
                {
                    Monitor.Wait(semaphore, timeout);
                }
                if (queue.Count > 0)
                {
                    return (IMessage) queue.Dequeue();
                }
            }
            return null;
        }
        
        /// <summary>
        /// Method Dequeue
        /// </summary>
        public IMessage Dequeue()
        {
            lock (semaphore)
            {
                return (IMessage) queue.Dequeue();
            }
        }
        
    }
}
