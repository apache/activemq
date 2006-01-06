using System;

namespace OpenWire.Core
{
	/// <summary>
	/// Summary description for ActiveMQQueue.
	/// </summary>
	public class ActiveMQQueue : ActiveMQDestination {
		public ActiveMQQueue() : base(){}
		public ActiveMQQueue(String name) : base(name){}
		
		public String GetQueueName() {
			return base.GetPhysicalName();
		}
		
		public override int GetDestinationType() {
			return ACTIVEMQ_QUEUE;
		}

		public override ActiveMQDestination CreateDestination(String name) {
			return new ActiveMQQueue(name);
		}
	}
}