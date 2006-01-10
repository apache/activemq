using System;
using OpenWire.Core;
using OpenWire.Core.Commands;

namespace OpenWire.Core {
        /// <summary>
        /// Summary description for ActiveMQTopic.
        /// </summary>
        public class ActiveMQTopic : ActiveMQDestination, Topic {
                public const byte ID_ActiveMQTopic = 101;

                public ActiveMQTopic() : base() {
                }
                public ActiveMQTopic(String name) : base(name) {
                }

                public String TopicName {
                        get { return PhysicalName; } 
                }

                public override int GetDestinationType() {
                        return ACTIVEMQ_TOPIC; 
                }

                public override ActiveMQDestination CreateDestination(String name) {
                        return new ActiveMQTopic(name); 
                } 
        } 
}
