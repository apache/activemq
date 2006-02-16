using System;

namespace ActiveMQ
{
	/// <summary>
	/// Summary description for Packetpublic constants.
	/// </summary>
	 class PacketConstants
	{
		 PacketConstants()
		 {}

		public const int NOT_SET = 0;

    /**
     * ActiveMQMessage object
     */
     public const int ACTIVEMQ_MESSAGE = 6;

    /**
     * ActiveMQTextMessage object
     */

     public const int ACTIVEMQ_TEXT_MESSAGE = 7;

    /**
     * ActiveMQObjectMessage object
     */

     public const int ACTIVEMQ_OBJECT_MESSAGE = 8;

    /**
     * ActiveMQBytesMessage
     */

     public const int ACTIVEMQ_BYTES_MESSAGE = 9;

    /**
     * ActiveMQStreamMessage object
     */

     public const int ACTIVEMQ_STREAM_MESSAGE = 10;

    /**
     * ActiveMQMapMessage object
     */

     public const int ACTIVEMQ_MAP_MESSAGE = 11;

    /**
     * Message acknowledge
     */
     public const int ACTIVEMQ_MSG_ACK = 15;

    /**
     * Recipt message
     */

     public const int RECEIPT_INFO = 16;

    /**
     * Consumer Infomation
     */

     public const int CONSUMER_INFO = 17;

    /**
     * Producer Info
     */

     public const int PRODUCER_INFO = 18;

    /**
     * Transaction info
     */

     public const int TRANSACTION_INFO = 19;

    /**
     * XA Transaction info
     */

     public const int XA_TRANSACTION_INFO = 20;

    /**
     * Broker infomation message
     */

     public const int ACTIVEMQ_BROKER_INFO = 21;

    /**
     * Connection info message
     */

     public const int ACTIVEMQ_CONNECTION_INFO = 22;


    /**
     * Session Info message
     */
     public const int SESSION_INFO = 23;

    /**
     * Durable Unsubscribe message
     */

     public const int DURABLE_UNSUBSCRIBE = 24;


    /**
     * A receipt with an Object reponse.
     */
     public const int RESPONSE_RECEIPT_INFO = 25;


    /**
     * A receipt with an Integer reponse.
     */
     public const int INT_RESPONSE_RECEIPT_INFO = 26;

    /**
     * Infomation about the Capacity for more Messages for either Connection/Broker
     */
     public const int CAPACITY_INFO = 27;

    /**
     * Request infomation  about the current capacity
     */
     public const int CAPACITY_INFO_REQUEST = 28;
    
    /**
     * Infomation about the wire format expected
     */
     public const int WIRE_FORMAT_INFO = 29;

    /**
     * Keep-alive message
     */
     public const int KEEP_ALIVE = 30;
    
    /**
     * A command to the Broker Admin
     */
     public const int BROKER_ADMIN_COMMAND = 31;
    
    /**
     * transmit cached values for the wire format
     */
     public const int CACHED_VALUE_COMMAND = 32;

    /**
     * transmit cached values for the wire format
     */
     public const int CLEANUP_CONNECTION_INFO = 33;
		}
	}

