using System.Collections;
using OpenWire.Client.Commands;
using System;

namespace OpenWire.Client
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
            lock (queue) {
                if (queue.Peek() != null) {
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
