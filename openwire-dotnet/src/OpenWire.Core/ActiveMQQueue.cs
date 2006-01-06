using System;

namespace OpenWire.Core
{
	/// <summary>
	/// Summary description for ActiveMQQueue.
	/// </summary>
	public class ActiveMQQueue : ActiveMQDestination {
		public ActiveMQQueue() : base(){}
		public ActiveMQQueue(String name) : base(name){}
		public String getQueueName() {
			return base.getPhysicalName();
		}
		public override int getDestinationType() {
			return ACTIVEMQ_QUEUE;
		}

		public override ActiveMQDestination createDestination(String name) {
			return new ActiveMQQueue(name);
		}
	}
}