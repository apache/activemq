using System;
using OpenWire.Core;
using OpenWire.Core.Commands;

namespace OpenWire.Core {
        /// <summary>
        /// Summary description for ActiveMQQueue.
        /// </summary>
        public class ActiveMQQueue : ActiveMQDestination, Queue {
                public const byte ID_ActiveMQQueue = 100;

                public ActiveMQQueue() : base() {
                }
                public ActiveMQQueue(String name) : base(name) {
                }

                public String QueueName {
                        get { return PhysicalName; } 
                }

                public override int GetDestinationType() {
                        return ACTIVEMQ_QUEUE; 
                }

                public override ActiveMQDestination CreateDestination(String name) {
                        return new ActiveMQQueue(name); 
                } 
        } 
}
