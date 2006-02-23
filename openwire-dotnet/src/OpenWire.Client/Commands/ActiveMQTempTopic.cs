using System;
using OpenWire.Client;
using OpenWire.Client.Commands;
using OpenWire.Client.Core;

namespace OpenWire.Client.Commands
{
    /// <summary>
    /// Summary description for ActiveMQTempTopic.
    /// </summary>
    public class ActiveMQTempTopic : ActiveMQDestination, ITemporaryTopic
    {
        public const byte ID_ActiveMQTempTopic = 103;
        
        public ActiveMQTempTopic() : base()
        {
        }
        public ActiveMQTempTopic(String name) : base(name)
        {
        }
        
        public String GetTopicName()
        {
            return PhysicalName;
        }
        
        public override byte GetDataStructureType()
        {
            return ID_ActiveMQTempTopic;
        }
        
        public override int GetDestinationType()
        {
            return ACTIVEMQ_TOPIC;
        }
        
        public override ActiveMQDestination CreateDestination(String name)
        {
            return new ActiveMQTempTopic(name);
        }
    }
}
