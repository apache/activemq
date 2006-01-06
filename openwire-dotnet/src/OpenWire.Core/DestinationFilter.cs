using System;
using OpenWire.Core.Commands;

namespace OpenWire.Core
{
	/// <summary>
	/// Summary description for DestinationFilter.
	/// </summary>
	public abstract class DestinationFilter 
	{
		public const String ANY_DESCENDENT = ">";
		public const String ANY_CHILD = "*";

		public bool matches(ActiveMQMessage message) 
		{
			return matches(message.getJMSDestination());
		}

		public abstract bool matches(ActiveMQDestination destination);

    
	}

}
