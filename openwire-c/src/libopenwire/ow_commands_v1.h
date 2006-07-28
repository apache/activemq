/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*****************************************************************************************
 *  
 * NOTE!: This file is auto generated - do not modify!
 *        if you need to make a change, please see the modify the groovy scripts in the
 *        under src/gram/script and then use maven openwire:generate to regenerate 
 *        this file.
 *  
 *****************************************************************************************/
 
#ifndef OW_COMMANDS_V1_H
#define OW_COMMANDS_V1_H

#include "ow.h"
#include "ow_command_types_v1.h"

#ifdef __cplusplus
extern "C" {
#endif /* __cplusplus */
      
#define OW_WIREFORMAT_VERSION 1
      
apr_status_t ow_bitmarshall(ow_bit_buffer *buffer, ow_DataStructure *object);
apr_status_t ow_marshall(ow_byte_buffer *buffer, ow_DataStructure *object);

typedef struct ow_LocalTransactionId {

   ow_byte structType;
   ow_long value;
   struct ow_ConnectionId *connectionId;

} ow_LocalTransactionId;
ow_LocalTransactionId *ow_LocalTransactionId_create(apr_pool_t *pool);
ow_boolean ow_is_a_LocalTransactionId(ow_DataStructure *object);

typedef struct ow_PartialCommand {

   ow_byte structType;
   ow_int commandId;
   ow_byte_array *data;

} ow_PartialCommand;
ow_PartialCommand *ow_PartialCommand_create(apr_pool_t *pool);
ow_boolean ow_is_a_PartialCommand(ow_DataStructure *object);

typedef struct ow_IntegerResponse {

   ow_byte structType;
   ow_int commandId;
   ow_boolean responseRequired;
   ow_int correlationId;
   ow_int result;

} ow_IntegerResponse;
ow_IntegerResponse *ow_IntegerResponse_create(apr_pool_t *pool);
ow_boolean ow_is_a_IntegerResponse(ow_DataStructure *object);

typedef struct ow_ActiveMQQueue {

   ow_byte structType;
   ow_string *physicalName;

} ow_ActiveMQQueue;
ow_ActiveMQQueue *ow_ActiveMQQueue_create(apr_pool_t *pool);
ow_boolean ow_is_a_ActiveMQQueue(ow_DataStructure *object);

typedef struct ow_TransactionId {

   ow_byte structType;

} ow_TransactionId;
ow_TransactionId *ow_TransactionId_create(apr_pool_t *pool);
ow_boolean ow_is_a_TransactionId(ow_DataStructure *object);

typedef struct ow_ActiveMQObjectMessage {

   ow_byte structType;
   ow_int commandId;
   ow_boolean responseRequired;
   struct ow_ProducerId *producerId;
   struct ow_ActiveMQDestination *destination;
   struct ow_TransactionId *transactionId;
   struct ow_ActiveMQDestination *originalDestination;
   struct ow_MessageId *messageId;
   struct ow_TransactionId *originalTransactionId;
   ow_string *groupID;
   ow_int groupSequence;
   ow_string *correlationId;
   ow_boolean persistent;
   ow_long expiration;
   ow_byte priority;
   struct ow_ActiveMQDestination *replyTo;
   ow_long timestamp;
   ow_string *type;
   struct ow_ByteSequence *content;
   struct ow_ByteSequence *marshalledProperties;
   struct ow_DataStructure *dataStructure;
   struct ow_ConsumerId *targetConsumerId;
   ow_boolean compressed;
   ow_int redeliveryCounter;
   ow_DataStructure_array *brokerPath;
   ow_long arrival;
   ow_string *userID;
   ow_boolean recievedByDFBridge;

} ow_ActiveMQObjectMessage;
ow_ActiveMQObjectMessage *ow_ActiveMQObjectMessage_create(apr_pool_t *pool);
ow_boolean ow_is_a_ActiveMQObjectMessage(ow_DataStructure *object);

typedef struct ow_ConnectionId {

   ow_byte structType;
   ow_string *value;

} ow_ConnectionId;
ow_ConnectionId *ow_ConnectionId_create(apr_pool_t *pool);
ow_boolean ow_is_a_ConnectionId(ow_DataStructure *object);

typedef struct ow_ConnectionInfo {

   ow_byte structType;
   ow_int commandId;
   ow_boolean responseRequired;
   struct ow_ConnectionId *connectionId;
   ow_string *clientId;
   ow_string *password;
   ow_string *userName;
   ow_DataStructure_array *brokerPath;
   ow_boolean brokerMasterConnector;
   ow_boolean manageable;

} ow_ConnectionInfo;
ow_ConnectionInfo *ow_ConnectionInfo_create(apr_pool_t *pool);
ow_boolean ow_is_a_ConnectionInfo(ow_DataStructure *object);

typedef struct ow_ProducerInfo {

   ow_byte structType;
   ow_int commandId;
   ow_boolean responseRequired;
   struct ow_ProducerId *producerId;
   struct ow_ActiveMQDestination *destination;
   ow_DataStructure_array *brokerPath;

} ow_ProducerInfo;
ow_ProducerInfo *ow_ProducerInfo_create(apr_pool_t *pool);
ow_boolean ow_is_a_ProducerInfo(ow_DataStructure *object);

typedef struct ow_MessageDispatchNotification {

   ow_byte structType;
   ow_int commandId;
   ow_boolean responseRequired;
   struct ow_ConsumerId *consumerId;
   struct ow_ActiveMQDestination *destination;
   ow_long deliverySequenceId;
   struct ow_MessageId *messageId;

} ow_MessageDispatchNotification;
ow_MessageDispatchNotification *ow_MessageDispatchNotification_create(apr_pool_t *pool);
ow_boolean ow_is_a_MessageDispatchNotification(ow_DataStructure *object);

typedef struct ow_SessionInfo {

   ow_byte structType;
   ow_int commandId;
   ow_boolean responseRequired;
   struct ow_SessionId *sessionId;

} ow_SessionInfo;
ow_SessionInfo *ow_SessionInfo_create(apr_pool_t *pool);
ow_boolean ow_is_a_SessionInfo(ow_DataStructure *object);

typedef struct ow_TransactionInfo {

   ow_byte structType;
   ow_int commandId;
   ow_boolean responseRequired;
   struct ow_ConnectionId *connectionId;
   struct ow_TransactionId *transactionId;
   ow_byte type;

} ow_TransactionInfo;
ow_TransactionInfo *ow_TransactionInfo_create(apr_pool_t *pool);
ow_boolean ow_is_a_TransactionInfo(ow_DataStructure *object);

typedef struct ow_ActiveMQStreamMessage {

   ow_byte structType;
   ow_int commandId;
   ow_boolean responseRequired;
   struct ow_ProducerId *producerId;
   struct ow_ActiveMQDestination *destination;
   struct ow_TransactionId *transactionId;
   struct ow_ActiveMQDestination *originalDestination;
   struct ow_MessageId *messageId;
   struct ow_TransactionId *originalTransactionId;
   ow_string *groupID;
   ow_int groupSequence;
   ow_string *correlationId;
   ow_boolean persistent;
   ow_long expiration;
   ow_byte priority;
   struct ow_ActiveMQDestination *replyTo;
   ow_long timestamp;
   ow_string *type;
   struct ow_ByteSequence *content;
   struct ow_ByteSequence *marshalledProperties;
   struct ow_DataStructure *dataStructure;
   struct ow_ConsumerId *targetConsumerId;
   ow_boolean compressed;
   ow_int redeliveryCounter;
   ow_DataStructure_array *brokerPath;
   ow_long arrival;
   ow_string *userID;
   ow_boolean recievedByDFBridge;

} ow_ActiveMQStreamMessage;
ow_ActiveMQStreamMessage *ow_ActiveMQStreamMessage_create(apr_pool_t *pool);
ow_boolean ow_is_a_ActiveMQStreamMessage(ow_DataStructure *object);

typedef struct ow_MessageAck {

   ow_byte structType;
   ow_int commandId;
   ow_boolean responseRequired;
   struct ow_ActiveMQDestination *destination;
   struct ow_TransactionId *transactionId;
   struct ow_ConsumerId *consumerId;
   ow_byte ackType;
   struct ow_MessageId *firstMessageId;
   struct ow_MessageId *lastMessageId;
   ow_int messageCount;

} ow_MessageAck;
ow_MessageAck *ow_MessageAck_create(apr_pool_t *pool);
ow_boolean ow_is_a_MessageAck(ow_DataStructure *object);

typedef struct ow_ProducerId {

   ow_byte structType;
   ow_string *connectionId;
   ow_long value;
   ow_long sessionId;

} ow_ProducerId;
ow_ProducerId *ow_ProducerId_create(apr_pool_t *pool);
ow_boolean ow_is_a_ProducerId(ow_DataStructure *object);

typedef struct ow_MessageId {

   ow_byte structType;
   struct ow_ProducerId *producerId;
   ow_long producerSequenceId;
   ow_long brokerSequenceId;

} ow_MessageId;
ow_MessageId *ow_MessageId_create(apr_pool_t *pool);
ow_boolean ow_is_a_MessageId(ow_DataStructure *object);

typedef struct ow_ActiveMQTempQueue {

   ow_byte structType;
   ow_string *physicalName;

} ow_ActiveMQTempQueue;
ow_ActiveMQTempQueue *ow_ActiveMQTempQueue_create(apr_pool_t *pool);
ow_boolean ow_is_a_ActiveMQTempQueue(ow_DataStructure *object);

typedef struct ow_RemoveSubscriptionInfo {

   ow_byte structType;
   ow_int commandId;
   ow_boolean responseRequired;
   struct ow_ConnectionId *connectionId;
   ow_string *subcriptionName;
   ow_string *clientId;

} ow_RemoveSubscriptionInfo;
ow_RemoveSubscriptionInfo *ow_RemoveSubscriptionInfo_create(apr_pool_t *pool);
ow_boolean ow_is_a_RemoveSubscriptionInfo(ow_DataStructure *object);

typedef struct ow_SessionId {

   ow_byte structType;
   ow_string *connectionId;
   ow_long value;

} ow_SessionId;
ow_SessionId *ow_SessionId_create(apr_pool_t *pool);
ow_boolean ow_is_a_SessionId(ow_DataStructure *object);

typedef struct ow_DataArrayResponse {

   ow_byte structType;
   ow_int commandId;
   ow_boolean responseRequired;
   ow_int correlationId;
   ow_DataStructure_array *data;

} ow_DataArrayResponse;
ow_DataArrayResponse *ow_DataArrayResponse_create(apr_pool_t *pool);
ow_boolean ow_is_a_DataArrayResponse(ow_DataStructure *object);

typedef struct ow_JournalQueueAck {

   ow_byte structType;
   struct ow_ActiveMQDestination *destination;
   struct ow_MessageAck *messageAck;

} ow_JournalQueueAck;
ow_JournalQueueAck *ow_JournalQueueAck_create(apr_pool_t *pool);
ow_boolean ow_is_a_JournalQueueAck(ow_DataStructure *object);

typedef struct ow_Response {

   ow_byte structType;
   ow_int commandId;
   ow_boolean responseRequired;
   ow_int correlationId;

} ow_Response;
ow_Response *ow_Response_create(apr_pool_t *pool);
ow_boolean ow_is_a_Response(ow_DataStructure *object);

typedef struct ow_ConnectionError {

   ow_byte structType;
   ow_int commandId;
   ow_boolean responseRequired;
   ow_throwable *exception;
   struct ow_ConnectionId *connectionId;

} ow_ConnectionError;
ow_ConnectionError *ow_ConnectionError_create(apr_pool_t *pool);
ow_boolean ow_is_a_ConnectionError(ow_DataStructure *object);

typedef struct ow_ConsumerInfo {

   ow_byte structType;
   ow_int commandId;
   ow_boolean responseRequired;
   struct ow_ConsumerId *consumerId;
   ow_boolean browser;
   struct ow_ActiveMQDestination *destination;
   ow_int prefetchSize;
   ow_int maximumPendingMessageLimit;
   ow_boolean dispatchAsync;
   ow_string *selector;
   ow_string *subcriptionName;
   ow_boolean noLocal;
   ow_boolean exclusive;
   ow_boolean retroactive;
   ow_byte priority;
   ow_DataStructure_array *brokerPath;
   struct ow_BooleanExpression *additionalPredicate;
   ow_boolean networkSubscription;
   ow_boolean optimizedAcknowledge;
   ow_boolean noRangeAcks;

} ow_ConsumerInfo;
ow_ConsumerInfo *ow_ConsumerInfo_create(apr_pool_t *pool);
ow_boolean ow_is_a_ConsumerInfo(ow_DataStructure *object);

typedef struct ow_XATransactionId {

   ow_byte structType;
   ow_int formatId;
   ow_byte_array *globalTransactionId;
   ow_byte_array *branchQualifier;

} ow_XATransactionId;
ow_XATransactionId *ow_XATransactionId_create(apr_pool_t *pool);
ow_boolean ow_is_a_XATransactionId(ow_DataStructure *object);

typedef struct ow_JournalTrace {

   ow_byte structType;
   ow_string *message;

} ow_JournalTrace;
ow_JournalTrace *ow_JournalTrace_create(apr_pool_t *pool);
ow_boolean ow_is_a_JournalTrace(ow_DataStructure *object);

typedef struct ow_ConsumerId {

   ow_byte structType;
   ow_string *connectionId;
   ow_long sessionId;
   ow_long value;

} ow_ConsumerId;
ow_ConsumerId *ow_ConsumerId_create(apr_pool_t *pool);
ow_boolean ow_is_a_ConsumerId(ow_DataStructure *object);

typedef struct ow_ActiveMQTextMessage {

   ow_byte structType;
   ow_int commandId;
   ow_boolean responseRequired;
   struct ow_ProducerId *producerId;
   struct ow_ActiveMQDestination *destination;
   struct ow_TransactionId *transactionId;
   struct ow_ActiveMQDestination *originalDestination;
   struct ow_MessageId *messageId;
   struct ow_TransactionId *originalTransactionId;
   ow_string *groupID;
   ow_int groupSequence;
   ow_string *correlationId;
   ow_boolean persistent;
   ow_long expiration;
   ow_byte priority;
   struct ow_ActiveMQDestination *replyTo;
   ow_long timestamp;
   ow_string *type;
   struct ow_ByteSequence *content;
   struct ow_ByteSequence *marshalledProperties;
   struct ow_DataStructure *dataStructure;
   struct ow_ConsumerId *targetConsumerId;
   ow_boolean compressed;
   ow_int redeliveryCounter;
   ow_DataStructure_array *brokerPath;
   ow_long arrival;
   ow_string *userID;
   ow_boolean recievedByDFBridge;

} ow_ActiveMQTextMessage;
ow_ActiveMQTextMessage *ow_ActiveMQTextMessage_create(apr_pool_t *pool);
ow_boolean ow_is_a_ActiveMQTextMessage(ow_DataStructure *object);

typedef struct ow_SubscriptionInfo {

   ow_byte structType;
   ow_string *clientId;
   struct ow_ActiveMQDestination *destination;
   ow_string *selector;
   ow_string *subcriptionName;

} ow_SubscriptionInfo;
ow_SubscriptionInfo *ow_SubscriptionInfo_create(apr_pool_t *pool);
ow_boolean ow_is_a_SubscriptionInfo(ow_DataStructure *object);

typedef struct ow_JournalTransaction {

   ow_byte structType;
   struct ow_TransactionId *transactionId;
   ow_byte type;
   ow_boolean wasPrepared;

} ow_JournalTransaction;
ow_JournalTransaction *ow_JournalTransaction_create(apr_pool_t *pool);
ow_boolean ow_is_a_JournalTransaction(ow_DataStructure *object);

typedef struct ow_ControlCommand {

   ow_byte structType;
   ow_int commandId;
   ow_boolean responseRequired;
   ow_string *command;

} ow_ControlCommand;
ow_ControlCommand *ow_ControlCommand_create(apr_pool_t *pool);
ow_boolean ow_is_a_ControlCommand(ow_DataStructure *object);

typedef struct ow_LastPartialCommand {

   ow_byte structType;
   ow_int commandId;
   ow_byte_array *data;

} ow_LastPartialCommand;
ow_LastPartialCommand *ow_LastPartialCommand_create(apr_pool_t *pool);
ow_boolean ow_is_a_LastPartialCommand(ow_DataStructure *object);

typedef struct ow_NetworkBridgeFilter {

   ow_byte structType;
   ow_int networkTTL;
   struct ow_BrokerId *networkBrokerId;

} ow_NetworkBridgeFilter;
ow_NetworkBridgeFilter *ow_NetworkBridgeFilter_create(apr_pool_t *pool);
ow_boolean ow_is_a_NetworkBridgeFilter(ow_DataStructure *object);

typedef struct ow_ActiveMQBytesMessage {

   ow_byte structType;
   ow_int commandId;
   ow_boolean responseRequired;
   struct ow_ProducerId *producerId;
   struct ow_ActiveMQDestination *destination;
   struct ow_TransactionId *transactionId;
   struct ow_ActiveMQDestination *originalDestination;
   struct ow_MessageId *messageId;
   struct ow_TransactionId *originalTransactionId;
   ow_string *groupID;
   ow_int groupSequence;
   ow_string *correlationId;
   ow_boolean persistent;
   ow_long expiration;
   ow_byte priority;
   struct ow_ActiveMQDestination *replyTo;
   ow_long timestamp;
   ow_string *type;
   struct ow_ByteSequence *content;
   struct ow_ByteSequence *marshalledProperties;
   struct ow_DataStructure *dataStructure;
   struct ow_ConsumerId *targetConsumerId;
   ow_boolean compressed;
   ow_int redeliveryCounter;
   ow_DataStructure_array *brokerPath;
   ow_long arrival;
   ow_string *userID;
   ow_boolean recievedByDFBridge;

} ow_ActiveMQBytesMessage;
ow_ActiveMQBytesMessage *ow_ActiveMQBytesMessage_create(apr_pool_t *pool);
ow_boolean ow_is_a_ActiveMQBytesMessage(ow_DataStructure *object);

typedef struct ow_WireFormatInfo {

   ow_byte structType;
   ow_byte_array *magic;
   ow_int version;
   struct ow_ByteSequence *marshalledProperties;

} ow_WireFormatInfo;
ow_WireFormatInfo *ow_WireFormatInfo_create(apr_pool_t *pool);
ow_boolean ow_is_a_WireFormatInfo(ow_DataStructure *object);

typedef struct ow_ActiveMQTempTopic {

   ow_byte structType;
   ow_string *physicalName;

} ow_ActiveMQTempTopic;
ow_ActiveMQTempTopic *ow_ActiveMQTempTopic_create(apr_pool_t *pool);
ow_boolean ow_is_a_ActiveMQTempTopic(ow_DataStructure *object);

typedef struct ow_DiscoveryEvent {

   ow_byte structType;
   ow_string *serviceName;
   ow_string *brokerName;

} ow_DiscoveryEvent;
ow_DiscoveryEvent *ow_DiscoveryEvent_create(apr_pool_t *pool);
ow_boolean ow_is_a_DiscoveryEvent(ow_DataStructure *object);

typedef struct ow_ActiveMQTempDestination {

   ow_byte structType;
   ow_string *physicalName;

} ow_ActiveMQTempDestination;
ow_ActiveMQTempDestination *ow_ActiveMQTempDestination_create(apr_pool_t *pool);
ow_boolean ow_is_a_ActiveMQTempDestination(ow_DataStructure *object);

typedef struct ow_ReplayCommand {

   ow_byte structType;
   ow_int commandId;
   ow_boolean responseRequired;
   ow_int firstNakNumber;
   ow_int lastNakNumber;

} ow_ReplayCommand;
ow_ReplayCommand *ow_ReplayCommand_create(apr_pool_t *pool);
ow_boolean ow_is_a_ReplayCommand(ow_DataStructure *object);

typedef struct ow_ActiveMQDestination {

   ow_byte structType;
   ow_string *physicalName;

} ow_ActiveMQDestination;
ow_ActiveMQDestination *ow_ActiveMQDestination_create(apr_pool_t *pool);
ow_boolean ow_is_a_ActiveMQDestination(ow_DataStructure *object);

typedef struct ow_ActiveMQTopic {

   ow_byte structType;
   ow_string *physicalName;

} ow_ActiveMQTopic;
ow_ActiveMQTopic *ow_ActiveMQTopic_create(apr_pool_t *pool);
ow_boolean ow_is_a_ActiveMQTopic(ow_DataStructure *object);

typedef struct ow_BrokerInfo {

   ow_byte structType;
   ow_int commandId;
   ow_boolean responseRequired;
   struct ow_BrokerId *brokerId;
   ow_string *brokerURL;
   ow_DataStructure_array *peerBrokerInfos;
   ow_string *brokerName;
   ow_boolean slaveBroker;
   ow_boolean masterBroker;
   ow_boolean faultTolerantConfiguration;

} ow_BrokerInfo;
ow_BrokerInfo *ow_BrokerInfo_create(apr_pool_t *pool);
ow_boolean ow_is_a_BrokerInfo(ow_DataStructure *object);

typedef struct ow_DestinationInfo {

   ow_byte structType;
   ow_int commandId;
   ow_boolean responseRequired;
   struct ow_ConnectionId *connectionId;
   struct ow_ActiveMQDestination *destination;
   ow_byte operationType;
   ow_long timeout;
   ow_DataStructure_array *brokerPath;

} ow_DestinationInfo;
ow_DestinationInfo *ow_DestinationInfo_create(apr_pool_t *pool);
ow_boolean ow_is_a_DestinationInfo(ow_DataStructure *object);

typedef struct ow_ShutdownInfo {

   ow_byte structType;
   ow_int commandId;
   ow_boolean responseRequired;

} ow_ShutdownInfo;
ow_ShutdownInfo *ow_ShutdownInfo_create(apr_pool_t *pool);
ow_boolean ow_is_a_ShutdownInfo(ow_DataStructure *object);

typedef struct ow_DataResponse {

   ow_byte structType;
   ow_int commandId;
   ow_boolean responseRequired;
   ow_int correlationId;
   struct ow_DataStructure *data;

} ow_DataResponse;
ow_DataResponse *ow_DataResponse_create(apr_pool_t *pool);
ow_boolean ow_is_a_DataResponse(ow_DataStructure *object);

typedef struct ow_ConnectionControl {

   ow_byte structType;
   ow_int commandId;
   ow_boolean responseRequired;
   ow_boolean close;
   ow_boolean exit;
   ow_boolean faultTolerant;
   ow_boolean resume;
   ow_boolean suspend;

} ow_ConnectionControl;
ow_ConnectionControl *ow_ConnectionControl_create(apr_pool_t *pool);
ow_boolean ow_is_a_ConnectionControl(ow_DataStructure *object);

typedef struct ow_KeepAliveInfo {

   ow_byte structType;
   ow_int commandId;
   ow_boolean responseRequired;

} ow_KeepAliveInfo;
ow_KeepAliveInfo *ow_KeepAliveInfo_create(apr_pool_t *pool);
ow_boolean ow_is_a_KeepAliveInfo(ow_DataStructure *object);

typedef struct ow_Message {

   ow_byte structType;
   ow_int commandId;
   ow_boolean responseRequired;
   struct ow_ProducerId *producerId;
   struct ow_ActiveMQDestination *destination;
   struct ow_TransactionId *transactionId;
   struct ow_ActiveMQDestination *originalDestination;
   struct ow_MessageId *messageId;
   struct ow_TransactionId *originalTransactionId;
   ow_string *groupID;
   ow_int groupSequence;
   ow_string *correlationId;
   ow_boolean persistent;
   ow_long expiration;
   ow_byte priority;
   struct ow_ActiveMQDestination *replyTo;
   ow_long timestamp;
   ow_string *type;
   struct ow_ByteSequence *content;
   struct ow_ByteSequence *marshalledProperties;
   struct ow_DataStructure *dataStructure;
   struct ow_ConsumerId *targetConsumerId;
   ow_boolean compressed;
   ow_int redeliveryCounter;
   ow_DataStructure_array *brokerPath;
   ow_long arrival;
   ow_string *userID;
   ow_boolean recievedByDFBridge;

} ow_Message;
ow_Message *ow_Message_create(apr_pool_t *pool);
ow_boolean ow_is_a_Message(ow_DataStructure *object);

typedef struct ow_BaseCommand {

   ow_byte structType;
   ow_int commandId;
   ow_boolean responseRequired;

} ow_BaseCommand;
ow_BaseCommand *ow_BaseCommand_create(apr_pool_t *pool);
ow_boolean ow_is_a_BaseCommand(ow_DataStructure *object);

typedef struct ow_FlushCommand {

   ow_byte structType;
   ow_int commandId;
   ow_boolean responseRequired;

} ow_FlushCommand;
ow_FlushCommand *ow_FlushCommand_create(apr_pool_t *pool);
ow_boolean ow_is_a_FlushCommand(ow_DataStructure *object);

typedef struct ow_ConsumerControl {

   ow_byte structType;
   ow_int commandId;
   ow_boolean responseRequired;
   ow_boolean close;
   struct ow_ConsumerId *consumerId;
   ow_int prefetch;

} ow_ConsumerControl;
ow_ConsumerControl *ow_ConsumerControl_create(apr_pool_t *pool);
ow_boolean ow_is_a_ConsumerControl(ow_DataStructure *object);

typedef struct ow_JournalTopicAck {

   ow_byte structType;
   struct ow_ActiveMQDestination *destination;
   struct ow_MessageId *messageId;
   ow_long messageSequenceId;
   ow_string *subscritionName;
   ow_string *clientId;
   struct ow_TransactionId *transactionId;

} ow_JournalTopicAck;
ow_JournalTopicAck *ow_JournalTopicAck_create(apr_pool_t *pool);
ow_boolean ow_is_a_JournalTopicAck(ow_DataStructure *object);

typedef struct ow_BrokerId {

   ow_byte structType;
   ow_string *value;

} ow_BrokerId;
ow_BrokerId *ow_BrokerId_create(apr_pool_t *pool);
ow_boolean ow_is_a_BrokerId(ow_DataStructure *object);

typedef struct ow_MessageDispatch {

   ow_byte structType;
   ow_int commandId;
   ow_boolean responseRequired;
   struct ow_ConsumerId *consumerId;
   struct ow_ActiveMQDestination *destination;
   struct ow_Message *message;
   ow_int redeliveryCounter;

} ow_MessageDispatch;
ow_MessageDispatch *ow_MessageDispatch_create(apr_pool_t *pool);
ow_boolean ow_is_a_MessageDispatch(ow_DataStructure *object);

typedef struct ow_ActiveMQMapMessage {

   ow_byte structType;
   ow_int commandId;
   ow_boolean responseRequired;
   struct ow_ProducerId *producerId;
   struct ow_ActiveMQDestination *destination;
   struct ow_TransactionId *transactionId;
   struct ow_ActiveMQDestination *originalDestination;
   struct ow_MessageId *messageId;
   struct ow_TransactionId *originalTransactionId;
   ow_string *groupID;
   ow_int groupSequence;
   ow_string *correlationId;
   ow_boolean persistent;
   ow_long expiration;
   ow_byte priority;
   struct ow_ActiveMQDestination *replyTo;
   ow_long timestamp;
   ow_string *type;
   struct ow_ByteSequence *content;
   struct ow_ByteSequence *marshalledProperties;
   struct ow_DataStructure *dataStructure;
   struct ow_ConsumerId *targetConsumerId;
   ow_boolean compressed;
   ow_int redeliveryCounter;
   ow_DataStructure_array *brokerPath;
   ow_long arrival;
   ow_string *userID;
   ow_boolean recievedByDFBridge;

} ow_ActiveMQMapMessage;
ow_ActiveMQMapMessage *ow_ActiveMQMapMessage_create(apr_pool_t *pool);
ow_boolean ow_is_a_ActiveMQMapMessage(ow_DataStructure *object);

typedef struct ow_ActiveMQMessage {

   ow_byte structType;
   ow_int commandId;
   ow_boolean responseRequired;
   struct ow_ProducerId *producerId;
   struct ow_ActiveMQDestination *destination;
   struct ow_TransactionId *transactionId;
   struct ow_ActiveMQDestination *originalDestination;
   struct ow_MessageId *messageId;
   struct ow_TransactionId *originalTransactionId;
   ow_string *groupID;
   ow_int groupSequence;
   ow_string *correlationId;
   ow_boolean persistent;
   ow_long expiration;
   ow_byte priority;
   struct ow_ActiveMQDestination *replyTo;
   ow_long timestamp;
   ow_string *type;
   struct ow_ByteSequence *content;
   struct ow_ByteSequence *marshalledProperties;
   struct ow_DataStructure *dataStructure;
   struct ow_ConsumerId *targetConsumerId;
   ow_boolean compressed;
   ow_int redeliveryCounter;
   ow_DataStructure_array *brokerPath;
   ow_long arrival;
   ow_string *userID;
   ow_boolean recievedByDFBridge;

} ow_ActiveMQMessage;
ow_ActiveMQMessage *ow_ActiveMQMessage_create(apr_pool_t *pool);
ow_boolean ow_is_a_ActiveMQMessage(ow_DataStructure *object);

typedef struct ow_RemoveInfo {

   ow_byte structType;
   ow_int commandId;
   ow_boolean responseRequired;
   struct ow_DataStructure *objectId;

} ow_RemoveInfo;
ow_RemoveInfo *ow_RemoveInfo_create(apr_pool_t *pool);
ow_boolean ow_is_a_RemoveInfo(ow_DataStructure *object);

typedef struct ow_ExceptionResponse {

   ow_byte structType;
   ow_int commandId;
   ow_boolean responseRequired;
   ow_int correlationId;
   ow_throwable *exception;

} ow_ExceptionResponse;
ow_ExceptionResponse *ow_ExceptionResponse_create(apr_pool_t *pool);
ow_boolean ow_is_a_ExceptionResponse(ow_DataStructure *object);

#ifdef __cplusplus
}
#endif

#endif  /* ! OW_COMMANDS_V1_H */
