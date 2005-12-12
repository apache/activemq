using System;

namespace ActiveMQ
{
	/// <summary>
	/// Summary description for MessageAcknowledge.
	/// </summary>
	public interface MessageAcknowledge 
	{
		void acknowledge(ActiveMQMessage caller);
	}

}
