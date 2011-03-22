/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.command;

/**
 * Holds the command id constants used by the command objects.
 * 
 * 
 */
public interface CommandTypes {

    // What is the latest version of the openwire protocol
    byte PROTOCOL_VERSION = 7;

    // A marshaling layer can use this type to specify a null object.
    byte NULL = 0;

    // /////////////////////////////////////////////////
    //
    // Info objects sent back and forth client/server when
    // setting up a client connection.
    //
    // /////////////////////////////////////////////////
    byte WIREFORMAT_INFO = 1;
    byte BROKER_INFO = 2;
    byte CONNECTION_INFO = 3;
    byte SESSION_INFO = 4;
    byte CONSUMER_INFO = 5;
    byte PRODUCER_INFO = 6;
    byte TRANSACTION_INFO = 7;
    byte DESTINATION_INFO = 8;
    byte REMOVE_SUBSCRIPTION_INFO = 9;
    byte KEEP_ALIVE_INFO = 10;
    byte SHUTDOWN_INFO = 11;
    byte REMOVE_INFO = 12;
    byte CONTROL_COMMAND = 14;
    byte FLUSH_COMMAND = 15;
    byte CONNECTION_ERROR = 16;
    byte CONSUMER_CONTROL = 17;
    byte CONNECTION_CONTROL = 18;

    // /////////////////////////////////////////////////
    //
    // Messages that go back and forth between the client
    // and the server.
    //
    // /////////////////////////////////////////////////
    byte PRODUCER_ACK = 19;
    byte MESSAGE_PULL = 20;
    byte MESSAGE_DISPATCH = 21;
    byte MESSAGE_ACK = 22;

    byte ACTIVEMQ_MESSAGE = 23;
    byte ACTIVEMQ_BYTES_MESSAGE = 24;
    byte ACTIVEMQ_MAP_MESSAGE = 25;
    byte ACTIVEMQ_OBJECT_MESSAGE = 26;
    byte ACTIVEMQ_STREAM_MESSAGE = 27;
    byte ACTIVEMQ_TEXT_MESSAGE = 28;
    byte ACTIVEMQ_BLOB_MESSAGE = 29;

    // /////////////////////////////////////////////////
    //
    // Command Response messages
    //
    // /////////////////////////////////////////////////
    byte RESPONSE = 30;
    byte EXCEPTION_RESPONSE = 31;
    byte DATA_RESPONSE = 32;
    byte DATA_ARRAY_RESPONSE = 33;
    byte INTEGER_RESPONSE = 34;

    // /////////////////////////////////////////////////
    //
    // Used by discovery
    //
    // /////////////////////////////////////////////////
    byte DISCOVERY_EVENT = 40;

    // /////////////////////////////////////////////////
    //
    // Command object used by the Journal
    //
    // /////////////////////////////////////////////////
    byte JOURNAL_ACK = 50;
    byte JOURNAL_REMOVE = 52;
    byte JOURNAL_TRACE = 53;
    byte JOURNAL_TRANSACTION = 54;
    byte DURABLE_SUBSCRIPTION_INFO = 55;

    // /////////////////////////////////////////////////
    //
    // Reliability and fragmentation
    //
    // /////////////////////////////////////////////////
    byte PARTIAL_COMMAND = 60;
    byte PARTIAL_LAST_COMMAND = 61;

    byte REPLAY = 65;

    // /////////////////////////////////////////////////
    //
    // Types used represent basic Java types.
    //
    // /////////////////////////////////////////////////
    byte BYTE_TYPE = 70;
    byte CHAR_TYPE = 71;
    byte SHORT_TYPE = 72;
    byte INTEGER_TYPE = 73;
    byte LONG_TYPE = 74;
    byte DOUBLE_TYPE = 75;
    byte FLOAT_TYPE = 76;
    byte STRING_TYPE = 77;
    byte BOOLEAN_TYPE = 78;
    byte BYTE_ARRAY_TYPE = 79;

    // /////////////////////////////////////////////////
    //
    // Broker to Broker command objects
    //
    // /////////////////////////////////////////////////

    byte MESSAGE_DISPATCH_NOTIFICATION = 90;
    byte NETWORK_BRIDGE_FILTER = 91;

    // /////////////////////////////////////////////////
    //
    // Data structures contained in the command objects.
    //
    // /////////////////////////////////////////////////
    byte ACTIVEMQ_QUEUE = 100;
    byte ACTIVEMQ_TOPIC = 101;
    byte ACTIVEMQ_TEMP_QUEUE = 102;
    byte ACTIVEMQ_TEMP_TOPIC = 103;

    byte MESSAGE_ID = 110;
    byte ACTIVEMQ_LOCAL_TRANSACTION_ID = 111;
    byte ACTIVEMQ_XA_TRANSACTION_ID = 112;

    byte CONNECTION_ID = 120;
    byte SESSION_ID = 121;
    byte CONSUMER_ID = 122;
    byte PRODUCER_ID = 123;
    byte BROKER_ID = 124;

}
