using System;
using OpenWire.Client;
using OpenWire.Client.Commands;
using OpenWire.Client.Core;

namespace OpenWire.Client.Commands
{
    /// <summary>
    /// Summary description for ActiveMQTopic.
    /// </summary>
    public class ActiveMQTopic : ActiveMQDestination, ITopic
    {
        public const byte ID_ActiveMQTopic = 101;
        
        public ActiveMQTopic() : base()
        {
        }
        public ActiveMQTopic(String name) : base(name)
        {
        }
        
        public String TopicName
        {
            get { return PhysicalName; }
        }
        
        public override byte GetDataStructureType()
        {
            return ID_ActiveMQTopic;
        }
        
        public override int GetDestinationType()
        {
            return ACTIVEMQ_TOPIC;
        }
        
        public override ActiveMQDestination CreateDestination(String name)
        {
            return new ActiveMQTopic(name);
        }
    }
}
