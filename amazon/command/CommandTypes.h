/*
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at
  
  http://www.apache.org/licenses/LICENSE-2.0
  
  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
*/


#ifndef CommandTypes_hpp
#define CommandTypes_hpp

namespace ActiveMQ {
  namespace Command {

    /**
     * Holds the command id constants used by the command objects.
     * 
     * @version $Revision$
     */
    class Types {
      public:
        // A marshaling layer can use this type to specify a null object.
        const static int NULLTYPE                          = 0;

        ///////////////////////////////////////////////////
        //
        // Info objects sent back and forth client/server when
        // setting up a client connection.
        //
        ///////////////////////////////////////////////////    
        const static int WIREFORMAT_INFO                   = 1;
        const static int BROKER_INFO                       = 2;
        const static int CONNECTION_INFO                   = 3;
        const static int SESSION_INFO                      = 4;
        const static int CONSUMER_INFO                     = 5;
        const static int PRODUCER_INFO                     = 6;
        const static int TRANSACTION_INFO                  = 7;
        const static int DESTINATION_INFO                  = 8;
        const static int REMOVE_SUBSCRIPTION_INFO          = 9;
        const static int KEEP_ALIVE_INFO                   = 10;
        const static int SHUTDOWN_INFO                     = 11;
        const static int REMOVE_INFO                       = 12;
        const static int CONTROL_COMMAND                   = 14;
        const static int FLUSH_COMMAND                     = 15;
        const static int CONNECTION_ERROR                  = 16;
    
        ///////////////////////////////////////////////////
        //
        // Messages that go back and forth between the client
        // and the server.
        //
        ///////////////////////////////////////////////////    
        const static int MESSAGE_DISPATCH                  = 21;
        const static int MESSAGE_ACK                       = 22;
    
        const static int ACTIVEMQ_MESSAGE                  = 23;
        const static int ACTIVEMQ_BYTES_MESSAGE            = 24;
        const static int ACTIVEMQ_MAP_MESSAGE              = 25;
        const static int ACTIVEMQ_OBJECT_MESSAGE           = 26;
        const static int ACTIVEMQ_STREAM_MESSAGE           = 27;
        const static int ACTIVEMQ_TEXT_MESSAGE             = 28;

        ///////////////////////////////////////////////////
        //
        // Command Response messages
        //
        ///////////////////////////////////////////////////    
        const static int RESPONSE                          = 30;
        const static int EXCEPTION_RESPONSE                = 31;
        const static int DATA_RESPONSE                     = 32;
        const static int DATA_ARRAY_RESPONSE               = 33;
        const static int INTEGER_RESPONSE                  = 34;


        ///////////////////////////////////////////////////
        //
        // Used by discovery
        //
        ///////////////////////////////////////////////////    
        const static int DISCOVERY_EVENT                   = 40;
    
        ///////////////////////////////////////////////////
        //
        // Command object used by the Journal
        //
        ///////////////////////////////////////////////////    
        const static int JOURNAL_ACK                       = 50;
        const static int JOURNAL_REMOVE                    = 52;
        const static int JOURNAL_TRACE                     = 53;
        const static int JOURNAL_TRANSACTION               = 54;
        const static int DURABLE_SUBSCRIPTION_INFO         = 55;


        ///////////////////////////////////////////////////
        //
        // Reliability and fragmentation
        //
        ///////////////////////////////////////////////////    
        const static int PARTIAL_COMMAND                   = 60;
        const static int PARTIAL_LAST_COMMAND              = 61;
    
        const static int REPLAY                            = 65;



    
        ///////////////////////////////////////////////////
        //
        // Types used represent basic Java types.
        //
        ///////////////////////////////////////////////////    
        const static int BYTE_TYPE                         = 70;
        const static int CHAR_TYPE                         = 71;
        const static int SHORT_TYPE                        = 72;
        const static int INTEGER_TYPE                      = 73;
        const static int LONG_TYPE                         = 74;
        const static int DOUBLE_TYPE                       = 75;
        const static int FLOAT_TYPE                        = 76;
        const static int STRING_TYPE                       = 77;
        const static int BOOLEAN_TYPE                      = 78;
        const static int BYTE_ARRAY_TYPE                   = 79;
    
        ///////////////////////////////////////////////////
        //
        // Broker to Broker command objects
        //
        /////////////////////////////////////////////////// 
    
        const static int MESSAGE_DISPATCH_NOTIFICATION     = 90;
        const static int NETWORK_BRIDGE_FILTER             = 91;
    
        ///////////////////////////////////////////////////
        //
        // Data structures contained in the command objects.
        //
        ///////////////////////////////////////////////////    
        const static int ACTIVEMQ_QUEUE                    = 100;
        const static int ACTIVEMQ_TOPIC                    = 101;
        const static int ACTIVEMQ_TEMP_QUEUE               = 102;
        const static int ACTIVEMQ_TEMP_TOPIC               = 103;
    
        const static int MESSAGE_ID                        = 110;
        const static int ACTIVEMQ_LOCAL_TRANSACTION_ID     = 111;
        const static int ACTIVEMQ_XA_TRANSACTION_ID        = 112;

        const static int CONNECTION_ID                     = 120;
        const static int SESSION_ID                        = 121;
        const static int CONSUMER_ID                       = 122;
        const static int PRODUCER_ID                       = 123;
        const static int BROKER_ID                         = 124;
    };
  };
};

#endif /*CommandTypes_hpp*/
