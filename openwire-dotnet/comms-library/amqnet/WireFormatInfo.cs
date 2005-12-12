using System;

namespace ActiveMQ
{
	/// <summary>
	/// Summary description for WireFormatInfo.
	/// </summary>
	public class WireFormatInfo : AbstractPacket 
								{
    
		public char[] MAGIC =  new char[]{'A','c','t','i','v','e','M','Q'};

		int version;
    

		/**
		 * Return the type of Packet
		 *
		 * @return integer representation of the type of Packet
		 */

		public new int getPacketType() 
		{
			return WIRE_FORMAT_INFO;
		}
    
		/**
		 * @return pretty print
		 */
		public override String ToString() 
		{
			return super.toString() + " WireFormatInfo{ " +
				"version = " + version +
				" }";
		}

   

		/**
		 * @return Returns the version.
		 */
		public int getVersion() 
		{
			return version;
		}
		/**
		 * @param version The version to set.
		 */
		public void setVersion(int version) 
		{
			this.version = version;
		}
	}

}
