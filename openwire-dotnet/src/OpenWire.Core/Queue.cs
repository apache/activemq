using System;
using OpenWire.Core.Commands;

namespace OpenWire.Core
{
	/// <summary>
	/// Summary description for Queue.
	/// </summary>
	public interface Queue : Destination {

		String QueueName
		{
			get;
		}
	}
}