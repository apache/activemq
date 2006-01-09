using System;
using OpenWire.Core.Commands;

namespace OpenWire.Core
{
	/// <summary>
	/// Summary description for Topic.
	/// </summary>
	public interface Topic : Destination 
	{

		String TopicName
		{
			get;
		}
	}
}
