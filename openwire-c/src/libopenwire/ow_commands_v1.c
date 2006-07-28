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


#include "ow_commands_v1.h"

#define SUCCESS_CHECK( f ) { apr_status_t rc=f; if(rc!=APR_SUCCESS) return rc; }


ow_boolean ow_is_a_LocalTransactionId(ow_DataStructure *object) {
   if( object == 0 )
      return 0;
      
   switch(object->structType) {
   case OW_LOCALTRANSACTIONID_TYPE:
      return 1;
   }
   return 0;
}


ow_LocalTransactionId *ow_LocalTransactionId_create(apr_pool_t *pool) 
{
   ow_LocalTransactionId *value = apr_pcalloc(pool,sizeof(ow_LocalTransactionId));
   if( value!=0 ) {
      ((ow_DataStructure*)value)->structType = OW_LOCALTRANSACTIONID_TYPE;
   }
   return value;
}


apr_status_t ow_marshal1_LocalTransactionId(ow_bit_buffer *buffer, ow_LocalTransactionId *object)
{
   ow_marshal1_TransactionId(buffer, (ow_TransactionId*)object);
   ow_marshal1_long(buffer, object->value);
   SUCCESS_CHECK(ow_marshal1_cached_object(buffer, (ow_DataStructure*)object->connectionId));
   
	return APR_SUCCESS;
}
apr_status_t ow_marshal2_LocalTransactionId(ow_byte_buffer *buffer, ow_bit_buffer *bitbuffer, ow_LocalTransactionId *object)
{
   ow_marshal2_TransactionId(buffer, bitbuffer, (ow_TransactionId*)object);   
   SUCCESS_CHECK(ow_marshal2_long(buffer, bitbuffer, object->value));
   SUCCESS_CHECK(ow_marshal2_cached_object(buffer, bitbuffer, (ow_DataStructure*)object->connectionId));
   
	return APR_SUCCESS;
}

apr_status_t ow_unmarshal_LocalTransactionId(ow_byte_array *buffer, ow_bit_buffer *bitbuffer, ow_LocalTransactionId *object, apr_pool_t *pool)
{
   ow_unmarshal_TransactionId(buffer, bitbuffer, (ow_TransactionId*)object, pool);   
   SUCCESS_CHECK(ow_unmarshal_long(buffer, bitbuffer, &object->value, pool));
   SUCCESS_CHECK(ow_unmarshal_cached_object(buffer, bitbuffer, (ow_DataStructure**)&object->connectionId, pool));
   
	return APR_SUCCESS;
}

ow_boolean ow_is_a_PartialCommand(ow_DataStructure *object) {
   if( object == 0 )
      return 0;
      
   switch(object->structType) {
   case OW_PARTIALCOMMAND_TYPE:
   case OW_LASTPARTIALCOMMAND_TYPE:
      return 1;
   }
   return 0;
}


ow_PartialCommand *ow_PartialCommand_create(apr_pool_t *pool) 
{
   ow_PartialCommand *value = apr_pcalloc(pool,sizeof(ow_PartialCommand));
   if( value!=0 ) {
      ((ow_DataStructure*)value)->structType = OW_PARTIALCOMMAND_TYPE;
   }
   return value;
}


apr_status_t ow_marshal1_PartialCommand(ow_bit_buffer *buffer, ow_PartialCommand *object)
{
   ow_marshal1_DataStructure(buffer, (ow_DataStructure*)object);
   
   
                        ow_bit_buffer_append(buffer,  object->data!=0 );
                        
   
	return APR_SUCCESS;
}
apr_status_t ow_marshal2_PartialCommand(ow_byte_buffer *buffer, ow_bit_buffer *bitbuffer, ow_PartialCommand *object)
{
   ow_marshal2_DataStructure(buffer, bitbuffer, (ow_DataStructure*)object);   
   SUCCESS_CHECK(ow_byte_buffer_append_int(buffer, object->commandId));
   SUCCESS_CHECK(ow_marshal2_byte_array(buffer, bitbuffer, object->data));
   
	return APR_SUCCESS;
}

apr_status_t ow_unmarshal_PartialCommand(ow_byte_array *buffer, ow_bit_buffer *bitbuffer, ow_PartialCommand *object, apr_pool_t *pool)
{
   ow_unmarshal_DataStructure(buffer, bitbuffer, (ow_DataStructure*)object, pool);   
   SUCCESS_CHECK(ow_byte_array_read_int(buffer, &object->commandId));
   SUCCESS_CHECK(ow_unmarshal_byte_array(buffer, bitbuffer, &object->data, pool));
   
	return APR_SUCCESS;
}

ow_boolean ow_is_a_IntegerResponse(ow_DataStructure *object) {
   if( object == 0 )
      return 0;
      
   switch(object->structType) {
   case OW_INTEGERRESPONSE_TYPE:
      return 1;
   }
   return 0;
}


ow_IntegerResponse *ow_IntegerResponse_create(apr_pool_t *pool) 
{
   ow_IntegerResponse *value = apr_pcalloc(pool,sizeof(ow_IntegerResponse));
   if( value!=0 ) {
      ((ow_DataStructure*)value)->structType = OW_INTEGERRESPONSE_TYPE;
   }
   return value;
}


apr_status_t ow_marshal1_IntegerResponse(ow_bit_buffer *buffer, ow_IntegerResponse *object)
{
   ow_marshal1_Response(buffer, (ow_Response*)object);
   
   
	return APR_SUCCESS;
}
apr_status_t ow_marshal2_IntegerResponse(ow_byte_buffer *buffer, ow_bit_buffer *bitbuffer, ow_IntegerResponse *object)
{
   ow_marshal2_Response(buffer, bitbuffer, (ow_Response*)object);   
   SUCCESS_CHECK(ow_byte_buffer_append_int(buffer, object->result));
   
	return APR_SUCCESS;
}

apr_status_t ow_unmarshal_IntegerResponse(ow_byte_array *buffer, ow_bit_buffer *bitbuffer, ow_IntegerResponse *object, apr_pool_t *pool)
{
   ow_unmarshal_Response(buffer, bitbuffer, (ow_Response*)object, pool);   
   SUCCESS_CHECK(ow_byte_array_read_int(buffer, &object->result));
   
	return APR_SUCCESS;
}

ow_boolean ow_is_a_ActiveMQQueue(ow_DataStructure *object) {
   if( object == 0 )
      return 0;
      
   switch(object->structType) {
   case OW_ACTIVEMQQUEUE_TYPE:
      return 1;
   }
   return 0;
}


ow_ActiveMQQueue *ow_ActiveMQQueue_create(apr_pool_t *pool) 
{
   ow_ActiveMQQueue *value = apr_pcalloc(pool,sizeof(ow_ActiveMQQueue));
   if( value!=0 ) {
      ((ow_DataStructure*)value)->structType = OW_ACTIVEMQQUEUE_TYPE;
   }
   return value;
}


apr_status_t ow_marshal1_ActiveMQQueue(ow_bit_buffer *buffer, ow_ActiveMQQueue *object)
{
   ow_marshal1_ActiveMQDestination(buffer, (ow_ActiveMQDestination*)object);
   
	return APR_SUCCESS;
}
apr_status_t ow_marshal2_ActiveMQQueue(ow_byte_buffer *buffer, ow_bit_buffer *bitbuffer, ow_ActiveMQQueue *object)
{
   ow_marshal2_ActiveMQDestination(buffer, bitbuffer, (ow_ActiveMQDestination*)object);   
   
	return APR_SUCCESS;
}

apr_status_t ow_unmarshal_ActiveMQQueue(ow_byte_array *buffer, ow_bit_buffer *bitbuffer, ow_ActiveMQQueue *object, apr_pool_t *pool)
{
   ow_unmarshal_ActiveMQDestination(buffer, bitbuffer, (ow_ActiveMQDestination*)object, pool);   
   
	return APR_SUCCESS;
}

ow_boolean ow_is_a_TransactionId(ow_DataStructure *object) {
   if( object == 0 )
      return 0;
      
   switch(object->structType) {
   case OW_LOCALTRANSACTIONID_TYPE:
   case OW_XATRANSACTIONID_TYPE:
      return 1;
   }
   return 0;
}


apr_status_t ow_marshal1_TransactionId(ow_bit_buffer *buffer, ow_TransactionId *object)
{
   ow_marshal1_DataStructure(buffer, (ow_DataStructure*)object);
   
	return APR_SUCCESS;
}
apr_status_t ow_marshal2_TransactionId(ow_byte_buffer *buffer, ow_bit_buffer *bitbuffer, ow_TransactionId *object)
{
   ow_marshal2_DataStructure(buffer, bitbuffer, (ow_DataStructure*)object);   
   
	return APR_SUCCESS;
}

apr_status_t ow_unmarshal_TransactionId(ow_byte_array *buffer, ow_bit_buffer *bitbuffer, ow_TransactionId *object, apr_pool_t *pool)
{
   ow_unmarshal_DataStructure(buffer, bitbuffer, (ow_DataStructure*)object, pool);   
   
	return APR_SUCCESS;
}

ow_boolean ow_is_a_ActiveMQObjectMessage(ow_DataStructure *object) {
   if( object == 0 )
      return 0;
      
   switch(object->structType) {
   case OW_ACTIVEMQOBJECTMESSAGE_TYPE:
      return 1;
   }
   return 0;
}


ow_ActiveMQObjectMessage *ow_ActiveMQObjectMessage_create(apr_pool_t *pool) 
{
   ow_ActiveMQObjectMessage *value = apr_pcalloc(pool,sizeof(ow_ActiveMQObjectMessage));
   if( value!=0 ) {
      ((ow_DataStructure*)value)->structType = OW_ACTIVEMQOBJECTMESSAGE_TYPE;
   }
   return value;
}


apr_status_t ow_marshal1_ActiveMQObjectMessage(ow_bit_buffer *buffer, ow_ActiveMQObjectMessage *object)
{
   ow_marshal1_ActiveMQMessage(buffer, (ow_ActiveMQMessage*)object);
   
	return APR_SUCCESS;
}
apr_status_t ow_marshal2_ActiveMQObjectMessage(ow_byte_buffer *buffer, ow_bit_buffer *bitbuffer, ow_ActiveMQObjectMessage *object)
{
   ow_marshal2_ActiveMQMessage(buffer, bitbuffer, (ow_ActiveMQMessage*)object);   
   
	return APR_SUCCESS;
}

apr_status_t ow_unmarshal_ActiveMQObjectMessage(ow_byte_array *buffer, ow_bit_buffer *bitbuffer, ow_ActiveMQObjectMessage *object, apr_pool_t *pool)
{
   ow_unmarshal_ActiveMQMessage(buffer, bitbuffer, (ow_ActiveMQMessage*)object, pool);   
   
	return APR_SUCCESS;
}

ow_boolean ow_is_a_ConnectionId(ow_DataStructure *object) {
   if( object == 0 )
      return 0;
      
   switch(object->structType) {
   case OW_CONNECTIONID_TYPE:
      return 1;
   }
   return 0;
}


ow_ConnectionId *ow_ConnectionId_create(apr_pool_t *pool) 
{
   ow_ConnectionId *value = apr_pcalloc(pool,sizeof(ow_ConnectionId));
   if( value!=0 ) {
      ((ow_DataStructure*)value)->structType = OW_CONNECTIONID_TYPE;
   }
   return value;
}


apr_status_t ow_marshal1_ConnectionId(ow_bit_buffer *buffer, ow_ConnectionId *object)
{
   ow_marshal1_DataStructure(buffer, (ow_DataStructure*)object);
   ow_marshal1_string(buffer, object->value);
   
	return APR_SUCCESS;
}
apr_status_t ow_marshal2_ConnectionId(ow_byte_buffer *buffer, ow_bit_buffer *bitbuffer, ow_ConnectionId *object)
{
   ow_marshal2_DataStructure(buffer, bitbuffer, (ow_DataStructure*)object);   
   SUCCESS_CHECK(ow_marshal2_string(buffer, bitbuffer, object->value));
   
	return APR_SUCCESS;
}

apr_status_t ow_unmarshal_ConnectionId(ow_byte_array *buffer, ow_bit_buffer *bitbuffer, ow_ConnectionId *object, apr_pool_t *pool)
{
   ow_unmarshal_DataStructure(buffer, bitbuffer, (ow_DataStructure*)object, pool);   
   SUCCESS_CHECK(ow_unmarshal_string(buffer, bitbuffer, &object->value, pool));
   
	return APR_SUCCESS;
}

ow_boolean ow_is_a_ConnectionInfo(ow_DataStructure *object) {
   if( object == 0 )
      return 0;
      
   switch(object->structType) {
   case OW_CONNECTIONINFO_TYPE:
      return 1;
   }
   return 0;
}


ow_ConnectionInfo *ow_ConnectionInfo_create(apr_pool_t *pool) 
{
   ow_ConnectionInfo *value = apr_pcalloc(pool,sizeof(ow_ConnectionInfo));
   if( value!=0 ) {
      ((ow_DataStructure*)value)->structType = OW_CONNECTIONINFO_TYPE;
   }
   return value;
}


apr_status_t ow_marshal1_ConnectionInfo(ow_bit_buffer *buffer, ow_ConnectionInfo *object)
{
   ow_marshal1_BaseCommand(buffer, (ow_BaseCommand*)object);
   SUCCESS_CHECK(ow_marshal1_cached_object(buffer, (ow_DataStructure*)object->connectionId));
   ow_marshal1_string(buffer, object->clientId);
   ow_marshal1_string(buffer, object->password);
   ow_marshal1_string(buffer, object->userName);
   SUCCESS_CHECK(ow_marshal1_DataStructure_array(buffer, object->brokerPath));
   ow_bit_buffer_append(buffer, object->brokerMasterConnector);
   ow_bit_buffer_append(buffer, object->manageable);
   
	return APR_SUCCESS;
}
apr_status_t ow_marshal2_ConnectionInfo(ow_byte_buffer *buffer, ow_bit_buffer *bitbuffer, ow_ConnectionInfo *object)
{
   ow_marshal2_BaseCommand(buffer, bitbuffer, (ow_BaseCommand*)object);   
   SUCCESS_CHECK(ow_marshal2_cached_object(buffer, bitbuffer, (ow_DataStructure*)object->connectionId));
   SUCCESS_CHECK(ow_marshal2_string(buffer, bitbuffer, object->clientId));
   SUCCESS_CHECK(ow_marshal2_string(buffer, bitbuffer, object->password));
   SUCCESS_CHECK(ow_marshal2_string(buffer, bitbuffer, object->userName));
   SUCCESS_CHECK(ow_marshal2_DataStructure_array(buffer, bitbuffer, object->brokerPath));
   ow_bit_buffer_read(bitbuffer);
   ow_bit_buffer_read(bitbuffer);
   
	return APR_SUCCESS;
}

apr_status_t ow_unmarshal_ConnectionInfo(ow_byte_array *buffer, ow_bit_buffer *bitbuffer, ow_ConnectionInfo *object, apr_pool_t *pool)
{
   ow_unmarshal_BaseCommand(buffer, bitbuffer, (ow_BaseCommand*)object, pool);   
   SUCCESS_CHECK(ow_unmarshal_cached_object(buffer, bitbuffer, (ow_DataStructure**)&object->connectionId, pool));
   SUCCESS_CHECK(ow_unmarshal_string(buffer, bitbuffer, &object->clientId, pool));
   SUCCESS_CHECK(ow_unmarshal_string(buffer, bitbuffer, &object->password, pool));
   SUCCESS_CHECK(ow_unmarshal_string(buffer, bitbuffer, &object->userName, pool));
   SUCCESS_CHECK(ow_unmarshal_DataStructure_array(buffer, bitbuffer, &object->brokerPath, pool));
   object->brokerMasterConnector = ow_bit_buffer_read(bitbuffer);
   object->manageable = ow_bit_buffer_read(bitbuffer);
   
	return APR_SUCCESS;
}

ow_boolean ow_is_a_ProducerInfo(ow_DataStructure *object) {
   if( object == 0 )
      return 0;
      
   switch(object->structType) {
   case OW_PRODUCERINFO_TYPE:
      return 1;
   }
   return 0;
}


ow_ProducerInfo *ow_ProducerInfo_create(apr_pool_t *pool) 
{
   ow_ProducerInfo *value = apr_pcalloc(pool,sizeof(ow_ProducerInfo));
   if( value!=0 ) {
      ((ow_DataStructure*)value)->structType = OW_PRODUCERINFO_TYPE;
   }
   return value;
}


apr_status_t ow_marshal1_ProducerInfo(ow_bit_buffer *buffer, ow_ProducerInfo *object)
{
   ow_marshal1_BaseCommand(buffer, (ow_BaseCommand*)object);
   SUCCESS_CHECK(ow_marshal1_cached_object(buffer, (ow_DataStructure*)object->producerId));
   SUCCESS_CHECK(ow_marshal1_cached_object(buffer, (ow_DataStructure*)object->destination));
   SUCCESS_CHECK(ow_marshal1_DataStructure_array(buffer, object->brokerPath));
   
	return APR_SUCCESS;
}
apr_status_t ow_marshal2_ProducerInfo(ow_byte_buffer *buffer, ow_bit_buffer *bitbuffer, ow_ProducerInfo *object)
{
   ow_marshal2_BaseCommand(buffer, bitbuffer, (ow_BaseCommand*)object);   
   SUCCESS_CHECK(ow_marshal2_cached_object(buffer, bitbuffer, (ow_DataStructure*)object->producerId));
   SUCCESS_CHECK(ow_marshal2_cached_object(buffer, bitbuffer, (ow_DataStructure*)object->destination));
   SUCCESS_CHECK(ow_marshal2_DataStructure_array(buffer, bitbuffer, object->brokerPath));
   
	return APR_SUCCESS;
}

apr_status_t ow_unmarshal_ProducerInfo(ow_byte_array *buffer, ow_bit_buffer *bitbuffer, ow_ProducerInfo *object, apr_pool_t *pool)
{
   ow_unmarshal_BaseCommand(buffer, bitbuffer, (ow_BaseCommand*)object, pool);   
   SUCCESS_CHECK(ow_unmarshal_cached_object(buffer, bitbuffer, (ow_DataStructure**)&object->producerId, pool));
   SUCCESS_CHECK(ow_unmarshal_cached_object(buffer, bitbuffer, (ow_DataStructure**)&object->destination, pool));
   SUCCESS_CHECK(ow_unmarshal_DataStructure_array(buffer, bitbuffer, &object->brokerPath, pool));
   
	return APR_SUCCESS;
}

ow_boolean ow_is_a_MessageDispatchNotification(ow_DataStructure *object) {
   if( object == 0 )
      return 0;
      
   switch(object->structType) {
   case OW_MESSAGEDISPATCHNOTIFICATION_TYPE:
      return 1;
   }
   return 0;
}


ow_MessageDispatchNotification *ow_MessageDispatchNotification_create(apr_pool_t *pool) 
{
   ow_MessageDispatchNotification *value = apr_pcalloc(pool,sizeof(ow_MessageDispatchNotification));
   if( value!=0 ) {
      ((ow_DataStructure*)value)->structType = OW_MESSAGEDISPATCHNOTIFICATION_TYPE;
   }
   return value;
}


apr_status_t ow_marshal1_MessageDispatchNotification(ow_bit_buffer *buffer, ow_MessageDispatchNotification *object)
{
   ow_marshal1_BaseCommand(buffer, (ow_BaseCommand*)object);
   SUCCESS_CHECK(ow_marshal1_cached_object(buffer, (ow_DataStructure*)object->consumerId));
   SUCCESS_CHECK(ow_marshal1_cached_object(buffer, (ow_DataStructure*)object->destination));
   ow_marshal1_long(buffer, object->deliverySequenceId);
   SUCCESS_CHECK(ow_marshal1_nested_object(buffer, (ow_DataStructure*)object->messageId));
   
	return APR_SUCCESS;
}
apr_status_t ow_marshal2_MessageDispatchNotification(ow_byte_buffer *buffer, ow_bit_buffer *bitbuffer, ow_MessageDispatchNotification *object)
{
   ow_marshal2_BaseCommand(buffer, bitbuffer, (ow_BaseCommand*)object);   
   SUCCESS_CHECK(ow_marshal2_cached_object(buffer, bitbuffer, (ow_DataStructure*)object->consumerId));
   SUCCESS_CHECK(ow_marshal2_cached_object(buffer, bitbuffer, (ow_DataStructure*)object->destination));
   SUCCESS_CHECK(ow_marshal2_long(buffer, bitbuffer, object->deliverySequenceId));
   SUCCESS_CHECK(ow_marshal2_nested_object(buffer, bitbuffer, (ow_DataStructure*)object->messageId));
   
	return APR_SUCCESS;
}

apr_status_t ow_unmarshal_MessageDispatchNotification(ow_byte_array *buffer, ow_bit_buffer *bitbuffer, ow_MessageDispatchNotification *object, apr_pool_t *pool)
{
   ow_unmarshal_BaseCommand(buffer, bitbuffer, (ow_BaseCommand*)object, pool);   
   SUCCESS_CHECK(ow_unmarshal_cached_object(buffer, bitbuffer, (ow_DataStructure**)&object->consumerId, pool));
   SUCCESS_CHECK(ow_unmarshal_cached_object(buffer, bitbuffer, (ow_DataStructure**)&object->destination, pool));
   SUCCESS_CHECK(ow_unmarshal_long(buffer, bitbuffer, &object->deliverySequenceId, pool));
   SUCCESS_CHECK(ow_unmarshal_nested_object(buffer, bitbuffer, (ow_DataStructure**)&object->messageId, pool));
   
	return APR_SUCCESS;
}

ow_boolean ow_is_a_SessionInfo(ow_DataStructure *object) {
   if( object == 0 )
      return 0;
      
   switch(object->structType) {
   case OW_SESSIONINFO_TYPE:
      return 1;
   }
   return 0;
}


ow_SessionInfo *ow_SessionInfo_create(apr_pool_t *pool) 
{
   ow_SessionInfo *value = apr_pcalloc(pool,sizeof(ow_SessionInfo));
   if( value!=0 ) {
      ((ow_DataStructure*)value)->structType = OW_SESSIONINFO_TYPE;
   }
   return value;
}


apr_status_t ow_marshal1_SessionInfo(ow_bit_buffer *buffer, ow_SessionInfo *object)
{
   ow_marshal1_BaseCommand(buffer, (ow_BaseCommand*)object);
   SUCCESS_CHECK(ow_marshal1_cached_object(buffer, (ow_DataStructure*)object->sessionId));
   
	return APR_SUCCESS;
}
apr_status_t ow_marshal2_SessionInfo(ow_byte_buffer *buffer, ow_bit_buffer *bitbuffer, ow_SessionInfo *object)
{
   ow_marshal2_BaseCommand(buffer, bitbuffer, (ow_BaseCommand*)object);   
   SUCCESS_CHECK(ow_marshal2_cached_object(buffer, bitbuffer, (ow_DataStructure*)object->sessionId));
   
	return APR_SUCCESS;
}

apr_status_t ow_unmarshal_SessionInfo(ow_byte_array *buffer, ow_bit_buffer *bitbuffer, ow_SessionInfo *object, apr_pool_t *pool)
{
   ow_unmarshal_BaseCommand(buffer, bitbuffer, (ow_BaseCommand*)object, pool);   
   SUCCESS_CHECK(ow_unmarshal_cached_object(buffer, bitbuffer, (ow_DataStructure**)&object->sessionId, pool));
   
	return APR_SUCCESS;
}

ow_boolean ow_is_a_TransactionInfo(ow_DataStructure *object) {
   if( object == 0 )
      return 0;
      
   switch(object->structType) {
   case OW_TRANSACTIONINFO_TYPE:
      return 1;
   }
   return 0;
}


ow_TransactionInfo *ow_TransactionInfo_create(apr_pool_t *pool) 
{
   ow_TransactionInfo *value = apr_pcalloc(pool,sizeof(ow_TransactionInfo));
   if( value!=0 ) {
      ((ow_DataStructure*)value)->structType = OW_TRANSACTIONINFO_TYPE;
   }
   return value;
}


apr_status_t ow_marshal1_TransactionInfo(ow_bit_buffer *buffer, ow_TransactionInfo *object)
{
   ow_marshal1_BaseCommand(buffer, (ow_BaseCommand*)object);
   SUCCESS_CHECK(ow_marshal1_cached_object(buffer, (ow_DataStructure*)object->connectionId));
   SUCCESS_CHECK(ow_marshal1_cached_object(buffer, (ow_DataStructure*)object->transactionId));
   
   
	return APR_SUCCESS;
}
apr_status_t ow_marshal2_TransactionInfo(ow_byte_buffer *buffer, ow_bit_buffer *bitbuffer, ow_TransactionInfo *object)
{
   ow_marshal2_BaseCommand(buffer, bitbuffer, (ow_BaseCommand*)object);   
   SUCCESS_CHECK(ow_marshal2_cached_object(buffer, bitbuffer, (ow_DataStructure*)object->connectionId));
   SUCCESS_CHECK(ow_marshal2_cached_object(buffer, bitbuffer, (ow_DataStructure*)object->transactionId));
   SUCCESS_CHECK(ow_byte_buffer_append_byte(buffer, object->type));
   
	return APR_SUCCESS;
}

apr_status_t ow_unmarshal_TransactionInfo(ow_byte_array *buffer, ow_bit_buffer *bitbuffer, ow_TransactionInfo *object, apr_pool_t *pool)
{
   ow_unmarshal_BaseCommand(buffer, bitbuffer, (ow_BaseCommand*)object, pool);   
   SUCCESS_CHECK(ow_unmarshal_cached_object(buffer, bitbuffer, (ow_DataStructure**)&object->connectionId, pool));
   SUCCESS_CHECK(ow_unmarshal_cached_object(buffer, bitbuffer, (ow_DataStructure**)&object->transactionId, pool));
   SUCCESS_CHECK(ow_byte_array_read_byte(buffer, &object->type));
   
	return APR_SUCCESS;
}

ow_boolean ow_is_a_ActiveMQStreamMessage(ow_DataStructure *object) {
   if( object == 0 )
      return 0;
      
   switch(object->structType) {
   case OW_ACTIVEMQSTREAMMESSAGE_TYPE:
      return 1;
   }
   return 0;
}


ow_ActiveMQStreamMessage *ow_ActiveMQStreamMessage_create(apr_pool_t *pool) 
{
   ow_ActiveMQStreamMessage *value = apr_pcalloc(pool,sizeof(ow_ActiveMQStreamMessage));
   if( value!=0 ) {
      ((ow_DataStructure*)value)->structType = OW_ACTIVEMQSTREAMMESSAGE_TYPE;
   }
   return value;
}


apr_status_t ow_marshal1_ActiveMQStreamMessage(ow_bit_buffer *buffer, ow_ActiveMQStreamMessage *object)
{
   ow_marshal1_ActiveMQMessage(buffer, (ow_ActiveMQMessage*)object);
   
	return APR_SUCCESS;
}
apr_status_t ow_marshal2_ActiveMQStreamMessage(ow_byte_buffer *buffer, ow_bit_buffer *bitbuffer, ow_ActiveMQStreamMessage *object)
{
   ow_marshal2_ActiveMQMessage(buffer, bitbuffer, (ow_ActiveMQMessage*)object);   
   
	return APR_SUCCESS;
}

apr_status_t ow_unmarshal_ActiveMQStreamMessage(ow_byte_array *buffer, ow_bit_buffer *bitbuffer, ow_ActiveMQStreamMessage *object, apr_pool_t *pool)
{
   ow_unmarshal_ActiveMQMessage(buffer, bitbuffer, (ow_ActiveMQMessage*)object, pool);   
   
	return APR_SUCCESS;
}

ow_boolean ow_is_a_MessageAck(ow_DataStructure *object) {
   if( object == 0 )
      return 0;
      
   switch(object->structType) {
   case OW_MESSAGEACK_TYPE:
      return 1;
   }
   return 0;
}


ow_MessageAck *ow_MessageAck_create(apr_pool_t *pool) 
{
   ow_MessageAck *value = apr_pcalloc(pool,sizeof(ow_MessageAck));
   if( value!=0 ) {
      ((ow_DataStructure*)value)->structType = OW_MESSAGEACK_TYPE;
   }
   return value;
}


apr_status_t ow_marshal1_MessageAck(ow_bit_buffer *buffer, ow_MessageAck *object)
{
   ow_marshal1_BaseCommand(buffer, (ow_BaseCommand*)object);
   SUCCESS_CHECK(ow_marshal1_cached_object(buffer, (ow_DataStructure*)object->destination));
   SUCCESS_CHECK(ow_marshal1_cached_object(buffer, (ow_DataStructure*)object->transactionId));
   SUCCESS_CHECK(ow_marshal1_cached_object(buffer, (ow_DataStructure*)object->consumerId));
   
   SUCCESS_CHECK(ow_marshal1_nested_object(buffer, (ow_DataStructure*)object->firstMessageId));
   SUCCESS_CHECK(ow_marshal1_nested_object(buffer, (ow_DataStructure*)object->lastMessageId));
   
   
	return APR_SUCCESS;
}
apr_status_t ow_marshal2_MessageAck(ow_byte_buffer *buffer, ow_bit_buffer *bitbuffer, ow_MessageAck *object)
{
   ow_marshal2_BaseCommand(buffer, bitbuffer, (ow_BaseCommand*)object);   
   SUCCESS_CHECK(ow_marshal2_cached_object(buffer, bitbuffer, (ow_DataStructure*)object->destination));
   SUCCESS_CHECK(ow_marshal2_cached_object(buffer, bitbuffer, (ow_DataStructure*)object->transactionId));
   SUCCESS_CHECK(ow_marshal2_cached_object(buffer, bitbuffer, (ow_DataStructure*)object->consumerId));
   SUCCESS_CHECK(ow_byte_buffer_append_byte(buffer, object->ackType));
   SUCCESS_CHECK(ow_marshal2_nested_object(buffer, bitbuffer, (ow_DataStructure*)object->firstMessageId));
   SUCCESS_CHECK(ow_marshal2_nested_object(buffer, bitbuffer, (ow_DataStructure*)object->lastMessageId));
   SUCCESS_CHECK(ow_byte_buffer_append_int(buffer, object->messageCount));
   
	return APR_SUCCESS;
}

apr_status_t ow_unmarshal_MessageAck(ow_byte_array *buffer, ow_bit_buffer *bitbuffer, ow_MessageAck *object, apr_pool_t *pool)
{
   ow_unmarshal_BaseCommand(buffer, bitbuffer, (ow_BaseCommand*)object, pool);   
   SUCCESS_CHECK(ow_unmarshal_cached_object(buffer, bitbuffer, (ow_DataStructure**)&object->destination, pool));
   SUCCESS_CHECK(ow_unmarshal_cached_object(buffer, bitbuffer, (ow_DataStructure**)&object->transactionId, pool));
   SUCCESS_CHECK(ow_unmarshal_cached_object(buffer, bitbuffer, (ow_DataStructure**)&object->consumerId, pool));
   SUCCESS_CHECK(ow_byte_array_read_byte(buffer, &object->ackType));
   SUCCESS_CHECK(ow_unmarshal_nested_object(buffer, bitbuffer, (ow_DataStructure**)&object->firstMessageId, pool));
   SUCCESS_CHECK(ow_unmarshal_nested_object(buffer, bitbuffer, (ow_DataStructure**)&object->lastMessageId, pool));
   SUCCESS_CHECK(ow_byte_array_read_int(buffer, &object->messageCount));
   
	return APR_SUCCESS;
}

ow_boolean ow_is_a_ProducerId(ow_DataStructure *object) {
   if( object == 0 )
      return 0;
      
   switch(object->structType) {
   case OW_PRODUCERID_TYPE:
      return 1;
   }
   return 0;
}


ow_ProducerId *ow_ProducerId_create(apr_pool_t *pool) 
{
   ow_ProducerId *value = apr_pcalloc(pool,sizeof(ow_ProducerId));
   if( value!=0 ) {
      ((ow_DataStructure*)value)->structType = OW_PRODUCERID_TYPE;
   }
   return value;
}


apr_status_t ow_marshal1_ProducerId(ow_bit_buffer *buffer, ow_ProducerId *object)
{
   ow_marshal1_DataStructure(buffer, (ow_DataStructure*)object);
   ow_marshal1_string(buffer, object->connectionId);
   ow_marshal1_long(buffer, object->value);
   ow_marshal1_long(buffer, object->sessionId);
   
	return APR_SUCCESS;
}
apr_status_t ow_marshal2_ProducerId(ow_byte_buffer *buffer, ow_bit_buffer *bitbuffer, ow_ProducerId *object)
{
   ow_marshal2_DataStructure(buffer, bitbuffer, (ow_DataStructure*)object);   
   SUCCESS_CHECK(ow_marshal2_string(buffer, bitbuffer, object->connectionId));
   SUCCESS_CHECK(ow_marshal2_long(buffer, bitbuffer, object->value));
   SUCCESS_CHECK(ow_marshal2_long(buffer, bitbuffer, object->sessionId));
   
	return APR_SUCCESS;
}

apr_status_t ow_unmarshal_ProducerId(ow_byte_array *buffer, ow_bit_buffer *bitbuffer, ow_ProducerId *object, apr_pool_t *pool)
{
   ow_unmarshal_DataStructure(buffer, bitbuffer, (ow_DataStructure*)object, pool);   
   SUCCESS_CHECK(ow_unmarshal_string(buffer, bitbuffer, &object->connectionId, pool));
   SUCCESS_CHECK(ow_unmarshal_long(buffer, bitbuffer, &object->value, pool));
   SUCCESS_CHECK(ow_unmarshal_long(buffer, bitbuffer, &object->sessionId, pool));
   
	return APR_SUCCESS;
}

ow_boolean ow_is_a_MessageId(ow_DataStructure *object) {
   if( object == 0 )
      return 0;
      
   switch(object->structType) {
   case OW_MESSAGEID_TYPE:
      return 1;
   }
   return 0;
}


ow_MessageId *ow_MessageId_create(apr_pool_t *pool) 
{
   ow_MessageId *value = apr_pcalloc(pool,sizeof(ow_MessageId));
   if( value!=0 ) {
      ((ow_DataStructure*)value)->structType = OW_MESSAGEID_TYPE;
   }
   return value;
}


apr_status_t ow_marshal1_MessageId(ow_bit_buffer *buffer, ow_MessageId *object)
{
   ow_marshal1_DataStructure(buffer, (ow_DataStructure*)object);
   SUCCESS_CHECK(ow_marshal1_cached_object(buffer, (ow_DataStructure*)object->producerId));
   ow_marshal1_long(buffer, object->producerSequenceId);
   ow_marshal1_long(buffer, object->brokerSequenceId);
   
	return APR_SUCCESS;
}
apr_status_t ow_marshal2_MessageId(ow_byte_buffer *buffer, ow_bit_buffer *bitbuffer, ow_MessageId *object)
{
   ow_marshal2_DataStructure(buffer, bitbuffer, (ow_DataStructure*)object);   
   SUCCESS_CHECK(ow_marshal2_cached_object(buffer, bitbuffer, (ow_DataStructure*)object->producerId));
   SUCCESS_CHECK(ow_marshal2_long(buffer, bitbuffer, object->producerSequenceId));
   SUCCESS_CHECK(ow_marshal2_long(buffer, bitbuffer, object->brokerSequenceId));
   
	return APR_SUCCESS;
}

apr_status_t ow_unmarshal_MessageId(ow_byte_array *buffer, ow_bit_buffer *bitbuffer, ow_MessageId *object, apr_pool_t *pool)
{
   ow_unmarshal_DataStructure(buffer, bitbuffer, (ow_DataStructure*)object, pool);   
   SUCCESS_CHECK(ow_unmarshal_cached_object(buffer, bitbuffer, (ow_DataStructure**)&object->producerId, pool));
   SUCCESS_CHECK(ow_unmarshal_long(buffer, bitbuffer, &object->producerSequenceId, pool));
   SUCCESS_CHECK(ow_unmarshal_long(buffer, bitbuffer, &object->brokerSequenceId, pool));
   
	return APR_SUCCESS;
}

ow_boolean ow_is_a_ActiveMQTempQueue(ow_DataStructure *object) {
   if( object == 0 )
      return 0;
      
   switch(object->structType) {
   case OW_ACTIVEMQTEMPQUEUE_TYPE:
      return 1;
   }
   return 0;
}


ow_ActiveMQTempQueue *ow_ActiveMQTempQueue_create(apr_pool_t *pool) 
{
   ow_ActiveMQTempQueue *value = apr_pcalloc(pool,sizeof(ow_ActiveMQTempQueue));
   if( value!=0 ) {
      ((ow_DataStructure*)value)->structType = OW_ACTIVEMQTEMPQUEUE_TYPE;
   }
   return value;
}


apr_status_t ow_marshal1_ActiveMQTempQueue(ow_bit_buffer *buffer, ow_ActiveMQTempQueue *object)
{
   ow_marshal1_ActiveMQTempDestination(buffer, (ow_ActiveMQTempDestination*)object);
   
	return APR_SUCCESS;
}
apr_status_t ow_marshal2_ActiveMQTempQueue(ow_byte_buffer *buffer, ow_bit_buffer *bitbuffer, ow_ActiveMQTempQueue *object)
{
   ow_marshal2_ActiveMQTempDestination(buffer, bitbuffer, (ow_ActiveMQTempDestination*)object);   
   
	return APR_SUCCESS;
}

apr_status_t ow_unmarshal_ActiveMQTempQueue(ow_byte_array *buffer, ow_bit_buffer *bitbuffer, ow_ActiveMQTempQueue *object, apr_pool_t *pool)
{
   ow_unmarshal_ActiveMQTempDestination(buffer, bitbuffer, (ow_ActiveMQTempDestination*)object, pool);   
   
	return APR_SUCCESS;
}

ow_boolean ow_is_a_RemoveSubscriptionInfo(ow_DataStructure *object) {
   if( object == 0 )
      return 0;
      
   switch(object->structType) {
   case OW_REMOVESUBSCRIPTIONINFO_TYPE:
      return 1;
   }
   return 0;
}


ow_RemoveSubscriptionInfo *ow_RemoveSubscriptionInfo_create(apr_pool_t *pool) 
{
   ow_RemoveSubscriptionInfo *value = apr_pcalloc(pool,sizeof(ow_RemoveSubscriptionInfo));
   if( value!=0 ) {
      ((ow_DataStructure*)value)->structType = OW_REMOVESUBSCRIPTIONINFO_TYPE;
   }
   return value;
}


apr_status_t ow_marshal1_RemoveSubscriptionInfo(ow_bit_buffer *buffer, ow_RemoveSubscriptionInfo *object)
{
   ow_marshal1_BaseCommand(buffer, (ow_BaseCommand*)object);
   SUCCESS_CHECK(ow_marshal1_cached_object(buffer, (ow_DataStructure*)object->connectionId));
   ow_marshal1_string(buffer, object->subcriptionName);
   ow_marshal1_string(buffer, object->clientId);
   
	return APR_SUCCESS;
}
apr_status_t ow_marshal2_RemoveSubscriptionInfo(ow_byte_buffer *buffer, ow_bit_buffer *bitbuffer, ow_RemoveSubscriptionInfo *object)
{
   ow_marshal2_BaseCommand(buffer, bitbuffer, (ow_BaseCommand*)object);   
   SUCCESS_CHECK(ow_marshal2_cached_object(buffer, bitbuffer, (ow_DataStructure*)object->connectionId));
   SUCCESS_CHECK(ow_marshal2_string(buffer, bitbuffer, object->subcriptionName));
   SUCCESS_CHECK(ow_marshal2_string(buffer, bitbuffer, object->clientId));
   
	return APR_SUCCESS;
}

apr_status_t ow_unmarshal_RemoveSubscriptionInfo(ow_byte_array *buffer, ow_bit_buffer *bitbuffer, ow_RemoveSubscriptionInfo *object, apr_pool_t *pool)
{
   ow_unmarshal_BaseCommand(buffer, bitbuffer, (ow_BaseCommand*)object, pool);   
   SUCCESS_CHECK(ow_unmarshal_cached_object(buffer, bitbuffer, (ow_DataStructure**)&object->connectionId, pool));
   SUCCESS_CHECK(ow_unmarshal_string(buffer, bitbuffer, &object->subcriptionName, pool));
   SUCCESS_CHECK(ow_unmarshal_string(buffer, bitbuffer, &object->clientId, pool));
   
	return APR_SUCCESS;
}

ow_boolean ow_is_a_SessionId(ow_DataStructure *object) {
   if( object == 0 )
      return 0;
      
   switch(object->structType) {
   case OW_SESSIONID_TYPE:
      return 1;
   }
   return 0;
}


ow_SessionId *ow_SessionId_create(apr_pool_t *pool) 
{
   ow_SessionId *value = apr_pcalloc(pool,sizeof(ow_SessionId));
   if( value!=0 ) {
      ((ow_DataStructure*)value)->structType = OW_SESSIONID_TYPE;
   }
   return value;
}


apr_status_t ow_marshal1_SessionId(ow_bit_buffer *buffer, ow_SessionId *object)
{
   ow_marshal1_DataStructure(buffer, (ow_DataStructure*)object);
   ow_marshal1_string(buffer, object->connectionId);
   ow_marshal1_long(buffer, object->value);
   
	return APR_SUCCESS;
}
apr_status_t ow_marshal2_SessionId(ow_byte_buffer *buffer, ow_bit_buffer *bitbuffer, ow_SessionId *object)
{
   ow_marshal2_DataStructure(buffer, bitbuffer, (ow_DataStructure*)object);   
   SUCCESS_CHECK(ow_marshal2_string(buffer, bitbuffer, object->connectionId));
   SUCCESS_CHECK(ow_marshal2_long(buffer, bitbuffer, object->value));
   
	return APR_SUCCESS;
}

apr_status_t ow_unmarshal_SessionId(ow_byte_array *buffer, ow_bit_buffer *bitbuffer, ow_SessionId *object, apr_pool_t *pool)
{
   ow_unmarshal_DataStructure(buffer, bitbuffer, (ow_DataStructure*)object, pool);   
   SUCCESS_CHECK(ow_unmarshal_string(buffer, bitbuffer, &object->connectionId, pool));
   SUCCESS_CHECK(ow_unmarshal_long(buffer, bitbuffer, &object->value, pool));
   
	return APR_SUCCESS;
}

ow_boolean ow_is_a_DataArrayResponse(ow_DataStructure *object) {
   if( object == 0 )
      return 0;
      
   switch(object->structType) {
   case OW_DATAARRAYRESPONSE_TYPE:
      return 1;
   }
   return 0;
}


ow_DataArrayResponse *ow_DataArrayResponse_create(apr_pool_t *pool) 
{
   ow_DataArrayResponse *value = apr_pcalloc(pool,sizeof(ow_DataArrayResponse));
   if( value!=0 ) {
      ((ow_DataStructure*)value)->structType = OW_DATAARRAYRESPONSE_TYPE;
   }
   return value;
}


apr_status_t ow_marshal1_DataArrayResponse(ow_bit_buffer *buffer, ow_DataArrayResponse *object)
{
   ow_marshal1_Response(buffer, (ow_Response*)object);
   SUCCESS_CHECK(ow_marshal1_DataStructure_array(buffer, object->data));
   
	return APR_SUCCESS;
}
apr_status_t ow_marshal2_DataArrayResponse(ow_byte_buffer *buffer, ow_bit_buffer *bitbuffer, ow_DataArrayResponse *object)
{
   ow_marshal2_Response(buffer, bitbuffer, (ow_Response*)object);   
   SUCCESS_CHECK(ow_marshal2_DataStructure_array(buffer, bitbuffer, object->data));
   
	return APR_SUCCESS;
}

apr_status_t ow_unmarshal_DataArrayResponse(ow_byte_array *buffer, ow_bit_buffer *bitbuffer, ow_DataArrayResponse *object, apr_pool_t *pool)
{
   ow_unmarshal_Response(buffer, bitbuffer, (ow_Response*)object, pool);   
   SUCCESS_CHECK(ow_unmarshal_DataStructure_array(buffer, bitbuffer, &object->data, pool));
   
	return APR_SUCCESS;
}

ow_boolean ow_is_a_JournalQueueAck(ow_DataStructure *object) {
   if( object == 0 )
      return 0;
      
   switch(object->structType) {
   case OW_JOURNALQUEUEACK_TYPE:
      return 1;
   }
   return 0;
}


ow_JournalQueueAck *ow_JournalQueueAck_create(apr_pool_t *pool) 
{
   ow_JournalQueueAck *value = apr_pcalloc(pool,sizeof(ow_JournalQueueAck));
   if( value!=0 ) {
      ((ow_DataStructure*)value)->structType = OW_JOURNALQUEUEACK_TYPE;
   }
   return value;
}


apr_status_t ow_marshal1_JournalQueueAck(ow_bit_buffer *buffer, ow_JournalQueueAck *object)
{
   ow_marshal1_DataStructure(buffer, (ow_DataStructure*)object);
   SUCCESS_CHECK(ow_marshal1_nested_object(buffer, (ow_DataStructure*)object->destination));
   SUCCESS_CHECK(ow_marshal1_nested_object(buffer, (ow_DataStructure*)object->messageAck));
   
	return APR_SUCCESS;
}
apr_status_t ow_marshal2_JournalQueueAck(ow_byte_buffer *buffer, ow_bit_buffer *bitbuffer, ow_JournalQueueAck *object)
{
   ow_marshal2_DataStructure(buffer, bitbuffer, (ow_DataStructure*)object);   
   SUCCESS_CHECK(ow_marshal2_nested_object(buffer, bitbuffer, (ow_DataStructure*)object->destination));
   SUCCESS_CHECK(ow_marshal2_nested_object(buffer, bitbuffer, (ow_DataStructure*)object->messageAck));
   
	return APR_SUCCESS;
}

apr_status_t ow_unmarshal_JournalQueueAck(ow_byte_array *buffer, ow_bit_buffer *bitbuffer, ow_JournalQueueAck *object, apr_pool_t *pool)
{
   ow_unmarshal_DataStructure(buffer, bitbuffer, (ow_DataStructure*)object, pool);   
   SUCCESS_CHECK(ow_unmarshal_nested_object(buffer, bitbuffer, (ow_DataStructure**)&object->destination, pool));
   SUCCESS_CHECK(ow_unmarshal_nested_object(buffer, bitbuffer, (ow_DataStructure**)&object->messageAck, pool));
   
	return APR_SUCCESS;
}

ow_boolean ow_is_a_Response(ow_DataStructure *object) {
   if( object == 0 )
      return 0;
      
   switch(object->structType) {
   case OW_INTEGERRESPONSE_TYPE:
   case OW_DATAARRAYRESPONSE_TYPE:
   case OW_RESPONSE_TYPE:
   case OW_DATARESPONSE_TYPE:
   case OW_EXCEPTIONRESPONSE_TYPE:
      return 1;
   }
   return 0;
}


ow_Response *ow_Response_create(apr_pool_t *pool) 
{
   ow_Response *value = apr_pcalloc(pool,sizeof(ow_Response));
   if( value!=0 ) {
      ((ow_DataStructure*)value)->structType = OW_RESPONSE_TYPE;
   }
   return value;
}


apr_status_t ow_marshal1_Response(ow_bit_buffer *buffer, ow_Response *object)
{
   ow_marshal1_BaseCommand(buffer, (ow_BaseCommand*)object);
   
   
	return APR_SUCCESS;
}
apr_status_t ow_marshal2_Response(ow_byte_buffer *buffer, ow_bit_buffer *bitbuffer, ow_Response *object)
{
   ow_marshal2_BaseCommand(buffer, bitbuffer, (ow_BaseCommand*)object);   
   SUCCESS_CHECK(ow_byte_buffer_append_int(buffer, object->correlationId));
   
	return APR_SUCCESS;
}

apr_status_t ow_unmarshal_Response(ow_byte_array *buffer, ow_bit_buffer *bitbuffer, ow_Response *object, apr_pool_t *pool)
{
   ow_unmarshal_BaseCommand(buffer, bitbuffer, (ow_BaseCommand*)object, pool);   
   SUCCESS_CHECK(ow_byte_array_read_int(buffer, &object->correlationId));
   
	return APR_SUCCESS;
}

ow_boolean ow_is_a_ConnectionError(ow_DataStructure *object) {
   if( object == 0 )
      return 0;
      
   switch(object->structType) {
   case OW_CONNECTIONERROR_TYPE:
      return 1;
   }
   return 0;
}


ow_ConnectionError *ow_ConnectionError_create(apr_pool_t *pool) 
{
   ow_ConnectionError *value = apr_pcalloc(pool,sizeof(ow_ConnectionError));
   if( value!=0 ) {
      ((ow_DataStructure*)value)->structType = OW_CONNECTIONERROR_TYPE;
   }
   return value;
}


apr_status_t ow_marshal1_ConnectionError(ow_bit_buffer *buffer, ow_ConnectionError *object)
{
   ow_marshal1_BaseCommand(buffer, (ow_BaseCommand*)object);
   SUCCESS_CHECK(ow_marshal1_throwable(buffer, object->exception));
   SUCCESS_CHECK(ow_marshal1_nested_object(buffer, (ow_DataStructure*)object->connectionId));
   
	return APR_SUCCESS;
}
apr_status_t ow_marshal2_ConnectionError(ow_byte_buffer *buffer, ow_bit_buffer *bitbuffer, ow_ConnectionError *object)
{
   ow_marshal2_BaseCommand(buffer, bitbuffer, (ow_BaseCommand*)object);   
   SUCCESS_CHECK(ow_marshal2_throwable(buffer, bitbuffer, object->exception));
   SUCCESS_CHECK(ow_marshal2_nested_object(buffer, bitbuffer, (ow_DataStructure*)object->connectionId));
   
	return APR_SUCCESS;
}

apr_status_t ow_unmarshal_ConnectionError(ow_byte_array *buffer, ow_bit_buffer *bitbuffer, ow_ConnectionError *object, apr_pool_t *pool)
{
   ow_unmarshal_BaseCommand(buffer, bitbuffer, (ow_BaseCommand*)object, pool);   
   SUCCESS_CHECK(ow_unmarshal_throwable(buffer, bitbuffer, &object->exception, pool));
   SUCCESS_CHECK(ow_unmarshal_nested_object(buffer, bitbuffer, (ow_DataStructure**)&object->connectionId, pool));
   
	return APR_SUCCESS;
}

ow_boolean ow_is_a_ConsumerInfo(ow_DataStructure *object) {
   if( object == 0 )
      return 0;
      
   switch(object->structType) {
   case OW_CONSUMERINFO_TYPE:
      return 1;
   }
   return 0;
}


ow_ConsumerInfo *ow_ConsumerInfo_create(apr_pool_t *pool) 
{
   ow_ConsumerInfo *value = apr_pcalloc(pool,sizeof(ow_ConsumerInfo));
   if( value!=0 ) {
      ((ow_DataStructure*)value)->structType = OW_CONSUMERINFO_TYPE;
   }
   return value;
}


apr_status_t ow_marshal1_ConsumerInfo(ow_bit_buffer *buffer, ow_ConsumerInfo *object)
{
   ow_marshal1_BaseCommand(buffer, (ow_BaseCommand*)object);
   SUCCESS_CHECK(ow_marshal1_cached_object(buffer, (ow_DataStructure*)object->consumerId));
   ow_bit_buffer_append(buffer, object->browser);
   SUCCESS_CHECK(ow_marshal1_cached_object(buffer, (ow_DataStructure*)object->destination));
   
   
   ow_bit_buffer_append(buffer, object->dispatchAsync);
   ow_marshal1_string(buffer, object->selector);
   ow_marshal1_string(buffer, object->subcriptionName);
   ow_bit_buffer_append(buffer, object->noLocal);
   ow_bit_buffer_append(buffer, object->exclusive);
   ow_bit_buffer_append(buffer, object->retroactive);
   
   SUCCESS_CHECK(ow_marshal1_DataStructure_array(buffer, object->brokerPath));
   SUCCESS_CHECK(ow_marshal1_nested_object(buffer, (ow_DataStructure*)object->additionalPredicate));
   ow_bit_buffer_append(buffer, object->networkSubscription);
   ow_bit_buffer_append(buffer, object->optimizedAcknowledge);
   ow_bit_buffer_append(buffer, object->noRangeAcks);
   
	return APR_SUCCESS;
}
apr_status_t ow_marshal2_ConsumerInfo(ow_byte_buffer *buffer, ow_bit_buffer *bitbuffer, ow_ConsumerInfo *object)
{
   ow_marshal2_BaseCommand(buffer, bitbuffer, (ow_BaseCommand*)object);   
   SUCCESS_CHECK(ow_marshal2_cached_object(buffer, bitbuffer, (ow_DataStructure*)object->consumerId));
   ow_bit_buffer_read(bitbuffer);
   SUCCESS_CHECK(ow_marshal2_cached_object(buffer, bitbuffer, (ow_DataStructure*)object->destination));
   SUCCESS_CHECK(ow_byte_buffer_append_int(buffer, object->prefetchSize));
   SUCCESS_CHECK(ow_byte_buffer_append_int(buffer, object->maximumPendingMessageLimit));
   ow_bit_buffer_read(bitbuffer);
   SUCCESS_CHECK(ow_marshal2_string(buffer, bitbuffer, object->selector));
   SUCCESS_CHECK(ow_marshal2_string(buffer, bitbuffer, object->subcriptionName));
   ow_bit_buffer_read(bitbuffer);
   ow_bit_buffer_read(bitbuffer);
   ow_bit_buffer_read(bitbuffer);
   SUCCESS_CHECK(ow_byte_buffer_append_byte(buffer, object->priority));
   SUCCESS_CHECK(ow_marshal2_DataStructure_array(buffer, bitbuffer, object->brokerPath));
   SUCCESS_CHECK(ow_marshal2_nested_object(buffer, bitbuffer, (ow_DataStructure*)object->additionalPredicate));
   ow_bit_buffer_read(bitbuffer);
   ow_bit_buffer_read(bitbuffer);
   ow_bit_buffer_read(bitbuffer);
   
	return APR_SUCCESS;
}

apr_status_t ow_unmarshal_ConsumerInfo(ow_byte_array *buffer, ow_bit_buffer *bitbuffer, ow_ConsumerInfo *object, apr_pool_t *pool)
{
   ow_unmarshal_BaseCommand(buffer, bitbuffer, (ow_BaseCommand*)object, pool);   
   SUCCESS_CHECK(ow_unmarshal_cached_object(buffer, bitbuffer, (ow_DataStructure**)&object->consumerId, pool));
   object->browser = ow_bit_buffer_read(bitbuffer);
   SUCCESS_CHECK(ow_unmarshal_cached_object(buffer, bitbuffer, (ow_DataStructure**)&object->destination, pool));
   SUCCESS_CHECK(ow_byte_array_read_int(buffer, &object->prefetchSize));
   SUCCESS_CHECK(ow_byte_array_read_int(buffer, &object->maximumPendingMessageLimit));
   object->dispatchAsync = ow_bit_buffer_read(bitbuffer);
   SUCCESS_CHECK(ow_unmarshal_string(buffer, bitbuffer, &object->selector, pool));
   SUCCESS_CHECK(ow_unmarshal_string(buffer, bitbuffer, &object->subcriptionName, pool));
   object->noLocal = ow_bit_buffer_read(bitbuffer);
   object->exclusive = ow_bit_buffer_read(bitbuffer);
   object->retroactive = ow_bit_buffer_read(bitbuffer);
   SUCCESS_CHECK(ow_byte_array_read_byte(buffer, &object->priority));
   SUCCESS_CHECK(ow_unmarshal_DataStructure_array(buffer, bitbuffer, &object->brokerPath, pool));
   SUCCESS_CHECK(ow_unmarshal_nested_object(buffer, bitbuffer, (ow_DataStructure**)&object->additionalPredicate, pool));
   object->networkSubscription = ow_bit_buffer_read(bitbuffer);
   object->optimizedAcknowledge = ow_bit_buffer_read(bitbuffer);
   object->noRangeAcks = ow_bit_buffer_read(bitbuffer);
   
	return APR_SUCCESS;
}

ow_boolean ow_is_a_XATransactionId(ow_DataStructure *object) {
   if( object == 0 )
      return 0;
      
   switch(object->structType) {
   case OW_XATRANSACTIONID_TYPE:
      return 1;
   }
   return 0;
}


ow_XATransactionId *ow_XATransactionId_create(apr_pool_t *pool) 
{
   ow_XATransactionId *value = apr_pcalloc(pool,sizeof(ow_XATransactionId));
   if( value!=0 ) {
      ((ow_DataStructure*)value)->structType = OW_XATRANSACTIONID_TYPE;
   }
   return value;
}


apr_status_t ow_marshal1_XATransactionId(ow_bit_buffer *buffer, ow_XATransactionId *object)
{
   ow_marshal1_TransactionId(buffer, (ow_TransactionId*)object);
   
   
                        ow_bit_buffer_append(buffer,  object->globalTransactionId!=0 );
                        
   
                        ow_bit_buffer_append(buffer,  object->branchQualifier!=0 );
                        
   
	return APR_SUCCESS;
}
apr_status_t ow_marshal2_XATransactionId(ow_byte_buffer *buffer, ow_bit_buffer *bitbuffer, ow_XATransactionId *object)
{
   ow_marshal2_TransactionId(buffer, bitbuffer, (ow_TransactionId*)object);   
   SUCCESS_CHECK(ow_byte_buffer_append_int(buffer, object->formatId));
   SUCCESS_CHECK(ow_marshal2_byte_array(buffer, bitbuffer, object->globalTransactionId));
   SUCCESS_CHECK(ow_marshal2_byte_array(buffer, bitbuffer, object->branchQualifier));
   
	return APR_SUCCESS;
}

apr_status_t ow_unmarshal_XATransactionId(ow_byte_array *buffer, ow_bit_buffer *bitbuffer, ow_XATransactionId *object, apr_pool_t *pool)
{
   ow_unmarshal_TransactionId(buffer, bitbuffer, (ow_TransactionId*)object, pool);   
   SUCCESS_CHECK(ow_byte_array_read_int(buffer, &object->formatId));
   SUCCESS_CHECK(ow_unmarshal_byte_array(buffer, bitbuffer, &object->globalTransactionId, pool));
   SUCCESS_CHECK(ow_unmarshal_byte_array(buffer, bitbuffer, &object->branchQualifier, pool));
   
	return APR_SUCCESS;
}

ow_boolean ow_is_a_JournalTrace(ow_DataStructure *object) {
   if( object == 0 )
      return 0;
      
   switch(object->structType) {
   case OW_JOURNALTRACE_TYPE:
      return 1;
   }
   return 0;
}


ow_JournalTrace *ow_JournalTrace_create(apr_pool_t *pool) 
{
   ow_JournalTrace *value = apr_pcalloc(pool,sizeof(ow_JournalTrace));
   if( value!=0 ) {
      ((ow_DataStructure*)value)->structType = OW_JOURNALTRACE_TYPE;
   }
   return value;
}


apr_status_t ow_marshal1_JournalTrace(ow_bit_buffer *buffer, ow_JournalTrace *object)
{
   ow_marshal1_DataStructure(buffer, (ow_DataStructure*)object);
   ow_marshal1_string(buffer, object->message);
   
	return APR_SUCCESS;
}
apr_status_t ow_marshal2_JournalTrace(ow_byte_buffer *buffer, ow_bit_buffer *bitbuffer, ow_JournalTrace *object)
{
   ow_marshal2_DataStructure(buffer, bitbuffer, (ow_DataStructure*)object);   
   SUCCESS_CHECK(ow_marshal2_string(buffer, bitbuffer, object->message));
   
	return APR_SUCCESS;
}

apr_status_t ow_unmarshal_JournalTrace(ow_byte_array *buffer, ow_bit_buffer *bitbuffer, ow_JournalTrace *object, apr_pool_t *pool)
{
   ow_unmarshal_DataStructure(buffer, bitbuffer, (ow_DataStructure*)object, pool);   
   SUCCESS_CHECK(ow_unmarshal_string(buffer, bitbuffer, &object->message, pool));
   
	return APR_SUCCESS;
}

ow_boolean ow_is_a_ConsumerId(ow_DataStructure *object) {
   if( object == 0 )
      return 0;
      
   switch(object->structType) {
   case OW_CONSUMERID_TYPE:
      return 1;
   }
   return 0;
}


ow_ConsumerId *ow_ConsumerId_create(apr_pool_t *pool) 
{
   ow_ConsumerId *value = apr_pcalloc(pool,sizeof(ow_ConsumerId));
   if( value!=0 ) {
      ((ow_DataStructure*)value)->structType = OW_CONSUMERID_TYPE;
   }
   return value;
}


apr_status_t ow_marshal1_ConsumerId(ow_bit_buffer *buffer, ow_ConsumerId *object)
{
   ow_marshal1_DataStructure(buffer, (ow_DataStructure*)object);
   ow_marshal1_string(buffer, object->connectionId);
   ow_marshal1_long(buffer, object->sessionId);
   ow_marshal1_long(buffer, object->value);
   
	return APR_SUCCESS;
}
apr_status_t ow_marshal2_ConsumerId(ow_byte_buffer *buffer, ow_bit_buffer *bitbuffer, ow_ConsumerId *object)
{
   ow_marshal2_DataStructure(buffer, bitbuffer, (ow_DataStructure*)object);   
   SUCCESS_CHECK(ow_marshal2_string(buffer, bitbuffer, object->connectionId));
   SUCCESS_CHECK(ow_marshal2_long(buffer, bitbuffer, object->sessionId));
   SUCCESS_CHECK(ow_marshal2_long(buffer, bitbuffer, object->value));
   
	return APR_SUCCESS;
}

apr_status_t ow_unmarshal_ConsumerId(ow_byte_array *buffer, ow_bit_buffer *bitbuffer, ow_ConsumerId *object, apr_pool_t *pool)
{
   ow_unmarshal_DataStructure(buffer, bitbuffer, (ow_DataStructure*)object, pool);   
   SUCCESS_CHECK(ow_unmarshal_string(buffer, bitbuffer, &object->connectionId, pool));
   SUCCESS_CHECK(ow_unmarshal_long(buffer, bitbuffer, &object->sessionId, pool));
   SUCCESS_CHECK(ow_unmarshal_long(buffer, bitbuffer, &object->value, pool));
   
	return APR_SUCCESS;
}

ow_boolean ow_is_a_ActiveMQTextMessage(ow_DataStructure *object) {
   if( object == 0 )
      return 0;
      
   switch(object->structType) {
   case OW_ACTIVEMQTEXTMESSAGE_TYPE:
      return 1;
   }
   return 0;
}


ow_ActiveMQTextMessage *ow_ActiveMQTextMessage_create(apr_pool_t *pool) 
{
   ow_ActiveMQTextMessage *value = apr_pcalloc(pool,sizeof(ow_ActiveMQTextMessage));
   if( value!=0 ) {
      ((ow_DataStructure*)value)->structType = OW_ACTIVEMQTEXTMESSAGE_TYPE;
   }
   return value;
}


apr_status_t ow_marshal1_ActiveMQTextMessage(ow_bit_buffer *buffer, ow_ActiveMQTextMessage *object)
{
   ow_marshal1_ActiveMQMessage(buffer, (ow_ActiveMQMessage*)object);
   
	return APR_SUCCESS;
}
apr_status_t ow_marshal2_ActiveMQTextMessage(ow_byte_buffer *buffer, ow_bit_buffer *bitbuffer, ow_ActiveMQTextMessage *object)
{
   ow_marshal2_ActiveMQMessage(buffer, bitbuffer, (ow_ActiveMQMessage*)object);   
   
	return APR_SUCCESS;
}

apr_status_t ow_unmarshal_ActiveMQTextMessage(ow_byte_array *buffer, ow_bit_buffer *bitbuffer, ow_ActiveMQTextMessage *object, apr_pool_t *pool)
{
   ow_unmarshal_ActiveMQMessage(buffer, bitbuffer, (ow_ActiveMQMessage*)object, pool);   
   
	return APR_SUCCESS;
}

ow_boolean ow_is_a_SubscriptionInfo(ow_DataStructure *object) {
   if( object == 0 )
      return 0;
      
   switch(object->structType) {
   case OW_SUBSCRIPTIONINFO_TYPE:
      return 1;
   }
   return 0;
}


ow_SubscriptionInfo *ow_SubscriptionInfo_create(apr_pool_t *pool) 
{
   ow_SubscriptionInfo *value = apr_pcalloc(pool,sizeof(ow_SubscriptionInfo));
   if( value!=0 ) {
      ((ow_DataStructure*)value)->structType = OW_SUBSCRIPTIONINFO_TYPE;
   }
   return value;
}


apr_status_t ow_marshal1_SubscriptionInfo(ow_bit_buffer *buffer, ow_SubscriptionInfo *object)
{
   ow_marshal1_DataStructure(buffer, (ow_DataStructure*)object);
   ow_marshal1_string(buffer, object->clientId);
   SUCCESS_CHECK(ow_marshal1_cached_object(buffer, (ow_DataStructure*)object->destination));
   ow_marshal1_string(buffer, object->selector);
   ow_marshal1_string(buffer, object->subcriptionName);
   
	return APR_SUCCESS;
}
apr_status_t ow_marshal2_SubscriptionInfo(ow_byte_buffer *buffer, ow_bit_buffer *bitbuffer, ow_SubscriptionInfo *object)
{
   ow_marshal2_DataStructure(buffer, bitbuffer, (ow_DataStructure*)object);   
   SUCCESS_CHECK(ow_marshal2_string(buffer, bitbuffer, object->clientId));
   SUCCESS_CHECK(ow_marshal2_cached_object(buffer, bitbuffer, (ow_DataStructure*)object->destination));
   SUCCESS_CHECK(ow_marshal2_string(buffer, bitbuffer, object->selector));
   SUCCESS_CHECK(ow_marshal2_string(buffer, bitbuffer, object->subcriptionName));
   
	return APR_SUCCESS;
}

apr_status_t ow_unmarshal_SubscriptionInfo(ow_byte_array *buffer, ow_bit_buffer *bitbuffer, ow_SubscriptionInfo *object, apr_pool_t *pool)
{
   ow_unmarshal_DataStructure(buffer, bitbuffer, (ow_DataStructure*)object, pool);   
   SUCCESS_CHECK(ow_unmarshal_string(buffer, bitbuffer, &object->clientId, pool));
   SUCCESS_CHECK(ow_unmarshal_cached_object(buffer, bitbuffer, (ow_DataStructure**)&object->destination, pool));
   SUCCESS_CHECK(ow_unmarshal_string(buffer, bitbuffer, &object->selector, pool));
   SUCCESS_CHECK(ow_unmarshal_string(buffer, bitbuffer, &object->subcriptionName, pool));
   
	return APR_SUCCESS;
}

ow_boolean ow_is_a_JournalTransaction(ow_DataStructure *object) {
   if( object == 0 )
      return 0;
      
   switch(object->structType) {
   case OW_JOURNALTRANSACTION_TYPE:
      return 1;
   }
   return 0;
}


ow_JournalTransaction *ow_JournalTransaction_create(apr_pool_t *pool) 
{
   ow_JournalTransaction *value = apr_pcalloc(pool,sizeof(ow_JournalTransaction));
   if( value!=0 ) {
      ((ow_DataStructure*)value)->structType = OW_JOURNALTRANSACTION_TYPE;
   }
   return value;
}


apr_status_t ow_marshal1_JournalTransaction(ow_bit_buffer *buffer, ow_JournalTransaction *object)
{
   ow_marshal1_DataStructure(buffer, (ow_DataStructure*)object);
   SUCCESS_CHECK(ow_marshal1_nested_object(buffer, (ow_DataStructure*)object->transactionId));
   
   ow_bit_buffer_append(buffer, object->wasPrepared);
   
	return APR_SUCCESS;
}
apr_status_t ow_marshal2_JournalTransaction(ow_byte_buffer *buffer, ow_bit_buffer *bitbuffer, ow_JournalTransaction *object)
{
   ow_marshal2_DataStructure(buffer, bitbuffer, (ow_DataStructure*)object);   
   SUCCESS_CHECK(ow_marshal2_nested_object(buffer, bitbuffer, (ow_DataStructure*)object->transactionId));
   SUCCESS_CHECK(ow_byte_buffer_append_byte(buffer, object->type));
   ow_bit_buffer_read(bitbuffer);
   
	return APR_SUCCESS;
}

apr_status_t ow_unmarshal_JournalTransaction(ow_byte_array *buffer, ow_bit_buffer *bitbuffer, ow_JournalTransaction *object, apr_pool_t *pool)
{
   ow_unmarshal_DataStructure(buffer, bitbuffer, (ow_DataStructure*)object, pool);   
   SUCCESS_CHECK(ow_unmarshal_nested_object(buffer, bitbuffer, (ow_DataStructure**)&object->transactionId, pool));
   SUCCESS_CHECK(ow_byte_array_read_byte(buffer, &object->type));
   object->wasPrepared = ow_bit_buffer_read(bitbuffer);
   
	return APR_SUCCESS;
}

ow_boolean ow_is_a_ControlCommand(ow_DataStructure *object) {
   if( object == 0 )
      return 0;
      
   switch(object->structType) {
   case OW_CONTROLCOMMAND_TYPE:
      return 1;
   }
   return 0;
}


ow_ControlCommand *ow_ControlCommand_create(apr_pool_t *pool) 
{
   ow_ControlCommand *value = apr_pcalloc(pool,sizeof(ow_ControlCommand));
   if( value!=0 ) {
      ((ow_DataStructure*)value)->structType = OW_CONTROLCOMMAND_TYPE;
   }
   return value;
}


apr_status_t ow_marshal1_ControlCommand(ow_bit_buffer *buffer, ow_ControlCommand *object)
{
   ow_marshal1_BaseCommand(buffer, (ow_BaseCommand*)object);
   ow_marshal1_string(buffer, object->command);
   
	return APR_SUCCESS;
}
apr_status_t ow_marshal2_ControlCommand(ow_byte_buffer *buffer, ow_bit_buffer *bitbuffer, ow_ControlCommand *object)
{
   ow_marshal2_BaseCommand(buffer, bitbuffer, (ow_BaseCommand*)object);   
   SUCCESS_CHECK(ow_marshal2_string(buffer, bitbuffer, object->command));
   
	return APR_SUCCESS;
}

apr_status_t ow_unmarshal_ControlCommand(ow_byte_array *buffer, ow_bit_buffer *bitbuffer, ow_ControlCommand *object, apr_pool_t *pool)
{
   ow_unmarshal_BaseCommand(buffer, bitbuffer, (ow_BaseCommand*)object, pool);   
   SUCCESS_CHECK(ow_unmarshal_string(buffer, bitbuffer, &object->command, pool));
   
	return APR_SUCCESS;
}

ow_boolean ow_is_a_LastPartialCommand(ow_DataStructure *object) {
   if( object == 0 )
      return 0;
      
   switch(object->structType) {
   case OW_LASTPARTIALCOMMAND_TYPE:
      return 1;
   }
   return 0;
}


ow_LastPartialCommand *ow_LastPartialCommand_create(apr_pool_t *pool) 
{
   ow_LastPartialCommand *value = apr_pcalloc(pool,sizeof(ow_LastPartialCommand));
   if( value!=0 ) {
      ((ow_DataStructure*)value)->structType = OW_LASTPARTIALCOMMAND_TYPE;
   }
   return value;
}


apr_status_t ow_marshal1_LastPartialCommand(ow_bit_buffer *buffer, ow_LastPartialCommand *object)
{
   ow_marshal1_PartialCommand(buffer, (ow_PartialCommand*)object);
   
	return APR_SUCCESS;
}
apr_status_t ow_marshal2_LastPartialCommand(ow_byte_buffer *buffer, ow_bit_buffer *bitbuffer, ow_LastPartialCommand *object)
{
   ow_marshal2_PartialCommand(buffer, bitbuffer, (ow_PartialCommand*)object);   
   
	return APR_SUCCESS;
}

apr_status_t ow_unmarshal_LastPartialCommand(ow_byte_array *buffer, ow_bit_buffer *bitbuffer, ow_LastPartialCommand *object, apr_pool_t *pool)
{
   ow_unmarshal_PartialCommand(buffer, bitbuffer, (ow_PartialCommand*)object, pool);   
   
	return APR_SUCCESS;
}

ow_boolean ow_is_a_NetworkBridgeFilter(ow_DataStructure *object) {
   if( object == 0 )
      return 0;
      
   switch(object->structType) {
   case OW_NETWORKBRIDGEFILTER_TYPE:
      return 1;
   }
   return 0;
}


ow_NetworkBridgeFilter *ow_NetworkBridgeFilter_create(apr_pool_t *pool) 
{
   ow_NetworkBridgeFilter *value = apr_pcalloc(pool,sizeof(ow_NetworkBridgeFilter));
   if( value!=0 ) {
      ((ow_DataStructure*)value)->structType = OW_NETWORKBRIDGEFILTER_TYPE;
   }
   return value;
}


apr_status_t ow_marshal1_NetworkBridgeFilter(ow_bit_buffer *buffer, ow_NetworkBridgeFilter *object)
{
   ow_marshal1_DataStructure(buffer, (ow_DataStructure*)object);
   
   SUCCESS_CHECK(ow_marshal1_cached_object(buffer, (ow_DataStructure*)object->networkBrokerId));
   
	return APR_SUCCESS;
}
apr_status_t ow_marshal2_NetworkBridgeFilter(ow_byte_buffer *buffer, ow_bit_buffer *bitbuffer, ow_NetworkBridgeFilter *object)
{
   ow_marshal2_DataStructure(buffer, bitbuffer, (ow_DataStructure*)object);   
   SUCCESS_CHECK(ow_byte_buffer_append_int(buffer, object->networkTTL));
   SUCCESS_CHECK(ow_marshal2_cached_object(buffer, bitbuffer, (ow_DataStructure*)object->networkBrokerId));
   
	return APR_SUCCESS;
}

apr_status_t ow_unmarshal_NetworkBridgeFilter(ow_byte_array *buffer, ow_bit_buffer *bitbuffer, ow_NetworkBridgeFilter *object, apr_pool_t *pool)
{
   ow_unmarshal_DataStructure(buffer, bitbuffer, (ow_DataStructure*)object, pool);   
   SUCCESS_CHECK(ow_byte_array_read_int(buffer, &object->networkTTL));
   SUCCESS_CHECK(ow_unmarshal_cached_object(buffer, bitbuffer, (ow_DataStructure**)&object->networkBrokerId, pool));
   
	return APR_SUCCESS;
}

ow_boolean ow_is_a_ActiveMQBytesMessage(ow_DataStructure *object) {
   if( object == 0 )
      return 0;
      
   switch(object->structType) {
   case OW_ACTIVEMQBYTESMESSAGE_TYPE:
      return 1;
   }
   return 0;
}


ow_ActiveMQBytesMessage *ow_ActiveMQBytesMessage_create(apr_pool_t *pool) 
{
   ow_ActiveMQBytesMessage *value = apr_pcalloc(pool,sizeof(ow_ActiveMQBytesMessage));
   if( value!=0 ) {
      ((ow_DataStructure*)value)->structType = OW_ACTIVEMQBYTESMESSAGE_TYPE;
   }
   return value;
}


apr_status_t ow_marshal1_ActiveMQBytesMessage(ow_bit_buffer *buffer, ow_ActiveMQBytesMessage *object)
{
   ow_marshal1_ActiveMQMessage(buffer, (ow_ActiveMQMessage*)object);
   
	return APR_SUCCESS;
}
apr_status_t ow_marshal2_ActiveMQBytesMessage(ow_byte_buffer *buffer, ow_bit_buffer *bitbuffer, ow_ActiveMQBytesMessage *object)
{
   ow_marshal2_ActiveMQMessage(buffer, bitbuffer, (ow_ActiveMQMessage*)object);   
   
	return APR_SUCCESS;
}

apr_status_t ow_unmarshal_ActiveMQBytesMessage(ow_byte_array *buffer, ow_bit_buffer *bitbuffer, ow_ActiveMQBytesMessage *object, apr_pool_t *pool)
{
   ow_unmarshal_ActiveMQMessage(buffer, bitbuffer, (ow_ActiveMQMessage*)object, pool);   
   
	return APR_SUCCESS;
}

ow_boolean ow_is_a_WireFormatInfo(ow_DataStructure *object) {
   if( object == 0 )
      return 0;
      
   switch(object->structType) {
   case OW_WIREFORMATINFO_TYPE:
      return 1;
   }
   return 0;
}


ow_WireFormatInfo *ow_WireFormatInfo_create(apr_pool_t *pool) 
{
   ow_WireFormatInfo *value = apr_pcalloc(pool,sizeof(ow_WireFormatInfo));
   if( value!=0 ) {
      ((ow_DataStructure*)value)->structType = OW_WIREFORMATINFO_TYPE;
   }
   return value;
}


apr_status_t ow_marshal1_WireFormatInfo(ow_bit_buffer *buffer, ow_WireFormatInfo *object)
{
   ow_marshal1_DataStructure(buffer, (ow_DataStructure*)object);
   
   
   SUCCESS_CHECK(ow_marshal1_nested_object(buffer, (ow_DataStructure*)object->marshalledProperties));
   
	return APR_SUCCESS;
}
apr_status_t ow_marshal2_WireFormatInfo(ow_byte_buffer *buffer, ow_bit_buffer *bitbuffer, ow_WireFormatInfo *object)
{
   ow_marshal2_DataStructure(buffer, bitbuffer, (ow_DataStructure*)object);   
   SUCCESS_CHECK(ow_marshal2_byte_array_const_size(buffer, object->magic, 8));
   SUCCESS_CHECK(ow_byte_buffer_append_int(buffer, object->version));
   SUCCESS_CHECK(ow_marshal2_nested_object(buffer, bitbuffer, (ow_DataStructure*)object->marshalledProperties));
   
	return APR_SUCCESS;
}

apr_status_t ow_unmarshal_WireFormatInfo(ow_byte_array *buffer, ow_bit_buffer *bitbuffer, ow_WireFormatInfo *object, apr_pool_t *pool)
{
   ow_unmarshal_DataStructure(buffer, bitbuffer, (ow_DataStructure*)object, pool);   
   SUCCESS_CHECK(ow_unmarshal_byte_array_const_size(buffer, &object->magic, 8, pool));
   SUCCESS_CHECK(ow_byte_array_read_int(buffer, &object->version));
   SUCCESS_CHECK(ow_unmarshal_nested_object(buffer, bitbuffer, (ow_DataStructure**)&object->marshalledProperties, pool));
   
	return APR_SUCCESS;
}

ow_boolean ow_is_a_ActiveMQTempTopic(ow_DataStructure *object) {
   if( object == 0 )
      return 0;
      
   switch(object->structType) {
   case OW_ACTIVEMQTEMPTOPIC_TYPE:
      return 1;
   }
   return 0;
}


ow_ActiveMQTempTopic *ow_ActiveMQTempTopic_create(apr_pool_t *pool) 
{
   ow_ActiveMQTempTopic *value = apr_pcalloc(pool,sizeof(ow_ActiveMQTempTopic));
   if( value!=0 ) {
      ((ow_DataStructure*)value)->structType = OW_ACTIVEMQTEMPTOPIC_TYPE;
   }
   return value;
}


apr_status_t ow_marshal1_ActiveMQTempTopic(ow_bit_buffer *buffer, ow_ActiveMQTempTopic *object)
{
   ow_marshal1_ActiveMQTempDestination(buffer, (ow_ActiveMQTempDestination*)object);
   
	return APR_SUCCESS;
}
apr_status_t ow_marshal2_ActiveMQTempTopic(ow_byte_buffer *buffer, ow_bit_buffer *bitbuffer, ow_ActiveMQTempTopic *object)
{
   ow_marshal2_ActiveMQTempDestination(buffer, bitbuffer, (ow_ActiveMQTempDestination*)object);   
   
	return APR_SUCCESS;
}

apr_status_t ow_unmarshal_ActiveMQTempTopic(ow_byte_array *buffer, ow_bit_buffer *bitbuffer, ow_ActiveMQTempTopic *object, apr_pool_t *pool)
{
   ow_unmarshal_ActiveMQTempDestination(buffer, bitbuffer, (ow_ActiveMQTempDestination*)object, pool);   
   
	return APR_SUCCESS;
}

ow_boolean ow_is_a_DiscoveryEvent(ow_DataStructure *object) {
   if( object == 0 )
      return 0;
      
   switch(object->structType) {
   case OW_DISCOVERYEVENT_TYPE:
      return 1;
   }
   return 0;
}


ow_DiscoveryEvent *ow_DiscoveryEvent_create(apr_pool_t *pool) 
{
   ow_DiscoveryEvent *value = apr_pcalloc(pool,sizeof(ow_DiscoveryEvent));
   if( value!=0 ) {
      ((ow_DataStructure*)value)->structType = OW_DISCOVERYEVENT_TYPE;
   }
   return value;
}


apr_status_t ow_marshal1_DiscoveryEvent(ow_bit_buffer *buffer, ow_DiscoveryEvent *object)
{
   ow_marshal1_DataStructure(buffer, (ow_DataStructure*)object);
   ow_marshal1_string(buffer, object->serviceName);
   ow_marshal1_string(buffer, object->brokerName);
   
	return APR_SUCCESS;
}
apr_status_t ow_marshal2_DiscoveryEvent(ow_byte_buffer *buffer, ow_bit_buffer *bitbuffer, ow_DiscoveryEvent *object)
{
   ow_marshal2_DataStructure(buffer, bitbuffer, (ow_DataStructure*)object);   
   SUCCESS_CHECK(ow_marshal2_string(buffer, bitbuffer, object->serviceName));
   SUCCESS_CHECK(ow_marshal2_string(buffer, bitbuffer, object->brokerName));
   
	return APR_SUCCESS;
}

apr_status_t ow_unmarshal_DiscoveryEvent(ow_byte_array *buffer, ow_bit_buffer *bitbuffer, ow_DiscoveryEvent *object, apr_pool_t *pool)
{
   ow_unmarshal_DataStructure(buffer, bitbuffer, (ow_DataStructure*)object, pool);   
   SUCCESS_CHECK(ow_unmarshal_string(buffer, bitbuffer, &object->serviceName, pool));
   SUCCESS_CHECK(ow_unmarshal_string(buffer, bitbuffer, &object->brokerName, pool));
   
	return APR_SUCCESS;
}

ow_boolean ow_is_a_ActiveMQTempDestination(ow_DataStructure *object) {
   if( object == 0 )
      return 0;
      
   switch(object->structType) {
   case OW_ACTIVEMQTEMPQUEUE_TYPE:
   case OW_ACTIVEMQTEMPTOPIC_TYPE:
      return 1;
   }
   return 0;
}


apr_status_t ow_marshal1_ActiveMQTempDestination(ow_bit_buffer *buffer, ow_ActiveMQTempDestination *object)
{
   ow_marshal1_ActiveMQDestination(buffer, (ow_ActiveMQDestination*)object);
   
	return APR_SUCCESS;
}
apr_status_t ow_marshal2_ActiveMQTempDestination(ow_byte_buffer *buffer, ow_bit_buffer *bitbuffer, ow_ActiveMQTempDestination *object)
{
   ow_marshal2_ActiveMQDestination(buffer, bitbuffer, (ow_ActiveMQDestination*)object);   
   
	return APR_SUCCESS;
}

apr_status_t ow_unmarshal_ActiveMQTempDestination(ow_byte_array *buffer, ow_bit_buffer *bitbuffer, ow_ActiveMQTempDestination *object, apr_pool_t *pool)
{
   ow_unmarshal_ActiveMQDestination(buffer, bitbuffer, (ow_ActiveMQDestination*)object, pool);   
   
	return APR_SUCCESS;
}

ow_boolean ow_is_a_ReplayCommand(ow_DataStructure *object) {
   if( object == 0 )
      return 0;
      
   switch(object->structType) {
   case OW_REPLAYCOMMAND_TYPE:
      return 1;
   }
   return 0;
}


ow_ReplayCommand *ow_ReplayCommand_create(apr_pool_t *pool) 
{
   ow_ReplayCommand *value = apr_pcalloc(pool,sizeof(ow_ReplayCommand));
   if( value!=0 ) {
      ((ow_DataStructure*)value)->structType = OW_REPLAYCOMMAND_TYPE;
   }
   return value;
}


apr_status_t ow_marshal1_ReplayCommand(ow_bit_buffer *buffer, ow_ReplayCommand *object)
{
   ow_marshal1_BaseCommand(buffer, (ow_BaseCommand*)object);
   
   
   
	return APR_SUCCESS;
}
apr_status_t ow_marshal2_ReplayCommand(ow_byte_buffer *buffer, ow_bit_buffer *bitbuffer, ow_ReplayCommand *object)
{
   ow_marshal2_BaseCommand(buffer, bitbuffer, (ow_BaseCommand*)object);   
   SUCCESS_CHECK(ow_byte_buffer_append_int(buffer, object->firstNakNumber));
   SUCCESS_CHECK(ow_byte_buffer_append_int(buffer, object->lastNakNumber));
   
	return APR_SUCCESS;
}

apr_status_t ow_unmarshal_ReplayCommand(ow_byte_array *buffer, ow_bit_buffer *bitbuffer, ow_ReplayCommand *object, apr_pool_t *pool)
{
   ow_unmarshal_BaseCommand(buffer, bitbuffer, (ow_BaseCommand*)object, pool);   
   SUCCESS_CHECK(ow_byte_array_read_int(buffer, &object->firstNakNumber));
   SUCCESS_CHECK(ow_byte_array_read_int(buffer, &object->lastNakNumber));
   
	return APR_SUCCESS;
}

ow_boolean ow_is_a_ActiveMQDestination(ow_DataStructure *object) {
   if( object == 0 )
      return 0;
      
   switch(object->structType) {
   case OW_ACTIVEMQQUEUE_TYPE:
   case OW_ACTIVEMQTEMPQUEUE_TYPE:
   case OW_ACTIVEMQTEMPTOPIC_TYPE:
   case OW_ACTIVEMQTOPIC_TYPE:
      return 1;
   }
   return 0;
}


apr_status_t ow_marshal1_ActiveMQDestination(ow_bit_buffer *buffer, ow_ActiveMQDestination *object)
{
   ow_marshal1_DataStructure(buffer, (ow_DataStructure*)object);
   ow_marshal1_string(buffer, object->physicalName);
   
	return APR_SUCCESS;
}
apr_status_t ow_marshal2_ActiveMQDestination(ow_byte_buffer *buffer, ow_bit_buffer *bitbuffer, ow_ActiveMQDestination *object)
{
   ow_marshal2_DataStructure(buffer, bitbuffer, (ow_DataStructure*)object);   
   SUCCESS_CHECK(ow_marshal2_string(buffer, bitbuffer, object->physicalName));
   
	return APR_SUCCESS;
}

apr_status_t ow_unmarshal_ActiveMQDestination(ow_byte_array *buffer, ow_bit_buffer *bitbuffer, ow_ActiveMQDestination *object, apr_pool_t *pool)
{
   ow_unmarshal_DataStructure(buffer, bitbuffer, (ow_DataStructure*)object, pool);   
   SUCCESS_CHECK(ow_unmarshal_string(buffer, bitbuffer, &object->physicalName, pool));
   
	return APR_SUCCESS;
}

ow_boolean ow_is_a_ActiveMQTopic(ow_DataStructure *object) {
   if( object == 0 )
      return 0;
      
   switch(object->structType) {
   case OW_ACTIVEMQTOPIC_TYPE:
      return 1;
   }
   return 0;
}


ow_ActiveMQTopic *ow_ActiveMQTopic_create(apr_pool_t *pool) 
{
   ow_ActiveMQTopic *value = apr_pcalloc(pool,sizeof(ow_ActiveMQTopic));
   if( value!=0 ) {
      ((ow_DataStructure*)value)->structType = OW_ACTIVEMQTOPIC_TYPE;
   }
   return value;
}


apr_status_t ow_marshal1_ActiveMQTopic(ow_bit_buffer *buffer, ow_ActiveMQTopic *object)
{
   ow_marshal1_ActiveMQDestination(buffer, (ow_ActiveMQDestination*)object);
   
	return APR_SUCCESS;
}
apr_status_t ow_marshal2_ActiveMQTopic(ow_byte_buffer *buffer, ow_bit_buffer *bitbuffer, ow_ActiveMQTopic *object)
{
   ow_marshal2_ActiveMQDestination(buffer, bitbuffer, (ow_ActiveMQDestination*)object);   
   
	return APR_SUCCESS;
}

apr_status_t ow_unmarshal_ActiveMQTopic(ow_byte_array *buffer, ow_bit_buffer *bitbuffer, ow_ActiveMQTopic *object, apr_pool_t *pool)
{
   ow_unmarshal_ActiveMQDestination(buffer, bitbuffer, (ow_ActiveMQDestination*)object, pool);   
   
	return APR_SUCCESS;
}

ow_boolean ow_is_a_BrokerInfo(ow_DataStructure *object) {
   if( object == 0 )
      return 0;
      
   switch(object->structType) {
   case OW_BROKERINFO_TYPE:
      return 1;
   }
   return 0;
}


ow_BrokerInfo *ow_BrokerInfo_create(apr_pool_t *pool) 
{
   ow_BrokerInfo *value = apr_pcalloc(pool,sizeof(ow_BrokerInfo));
   if( value!=0 ) {
      ((ow_DataStructure*)value)->structType = OW_BROKERINFO_TYPE;
   }
   return value;
}


apr_status_t ow_marshal1_BrokerInfo(ow_bit_buffer *buffer, ow_BrokerInfo *object)
{
   ow_marshal1_BaseCommand(buffer, (ow_BaseCommand*)object);
   SUCCESS_CHECK(ow_marshal1_cached_object(buffer, (ow_DataStructure*)object->brokerId));
   ow_marshal1_string(buffer, object->brokerURL);
   SUCCESS_CHECK(ow_marshal1_DataStructure_array(buffer, object->peerBrokerInfos));
   ow_marshal1_string(buffer, object->brokerName);
   ow_bit_buffer_append(buffer, object->slaveBroker);
   ow_bit_buffer_append(buffer, object->masterBroker);
   ow_bit_buffer_append(buffer, object->faultTolerantConfiguration);
   
	return APR_SUCCESS;
}
apr_status_t ow_marshal2_BrokerInfo(ow_byte_buffer *buffer, ow_bit_buffer *bitbuffer, ow_BrokerInfo *object)
{
   ow_marshal2_BaseCommand(buffer, bitbuffer, (ow_BaseCommand*)object);   
   SUCCESS_CHECK(ow_marshal2_cached_object(buffer, bitbuffer, (ow_DataStructure*)object->brokerId));
   SUCCESS_CHECK(ow_marshal2_string(buffer, bitbuffer, object->brokerURL));
   SUCCESS_CHECK(ow_marshal2_DataStructure_array(buffer, bitbuffer, object->peerBrokerInfos));
   SUCCESS_CHECK(ow_marshal2_string(buffer, bitbuffer, object->brokerName));
   ow_bit_buffer_read(bitbuffer);
   ow_bit_buffer_read(bitbuffer);
   ow_bit_buffer_read(bitbuffer);
   
	return APR_SUCCESS;
}

apr_status_t ow_unmarshal_BrokerInfo(ow_byte_array *buffer, ow_bit_buffer *bitbuffer, ow_BrokerInfo *object, apr_pool_t *pool)
{
   ow_unmarshal_BaseCommand(buffer, bitbuffer, (ow_BaseCommand*)object, pool);   
   SUCCESS_CHECK(ow_unmarshal_cached_object(buffer, bitbuffer, (ow_DataStructure**)&object->brokerId, pool));
   SUCCESS_CHECK(ow_unmarshal_string(buffer, bitbuffer, &object->brokerURL, pool));
   SUCCESS_CHECK(ow_unmarshal_DataStructure_array(buffer, bitbuffer, &object->peerBrokerInfos, pool));
   SUCCESS_CHECK(ow_unmarshal_string(buffer, bitbuffer, &object->brokerName, pool));
   object->slaveBroker = ow_bit_buffer_read(bitbuffer);
   object->masterBroker = ow_bit_buffer_read(bitbuffer);
   object->faultTolerantConfiguration = ow_bit_buffer_read(bitbuffer);
   
	return APR_SUCCESS;
}

ow_boolean ow_is_a_DestinationInfo(ow_DataStructure *object) {
   if( object == 0 )
      return 0;
      
   switch(object->structType) {
   case OW_DESTINATIONINFO_TYPE:
      return 1;
   }
   return 0;
}


ow_DestinationInfo *ow_DestinationInfo_create(apr_pool_t *pool) 
{
   ow_DestinationInfo *value = apr_pcalloc(pool,sizeof(ow_DestinationInfo));
   if( value!=0 ) {
      ((ow_DataStructure*)value)->structType = OW_DESTINATIONINFO_TYPE;
   }
   return value;
}


apr_status_t ow_marshal1_DestinationInfo(ow_bit_buffer *buffer, ow_DestinationInfo *object)
{
   ow_marshal1_BaseCommand(buffer, (ow_BaseCommand*)object);
   SUCCESS_CHECK(ow_marshal1_cached_object(buffer, (ow_DataStructure*)object->connectionId));
   SUCCESS_CHECK(ow_marshal1_cached_object(buffer, (ow_DataStructure*)object->destination));
   
   ow_marshal1_long(buffer, object->timeout);
   SUCCESS_CHECK(ow_marshal1_DataStructure_array(buffer, object->brokerPath));
   
	return APR_SUCCESS;
}
apr_status_t ow_marshal2_DestinationInfo(ow_byte_buffer *buffer, ow_bit_buffer *bitbuffer, ow_DestinationInfo *object)
{
   ow_marshal2_BaseCommand(buffer, bitbuffer, (ow_BaseCommand*)object);   
   SUCCESS_CHECK(ow_marshal2_cached_object(buffer, bitbuffer, (ow_DataStructure*)object->connectionId));
   SUCCESS_CHECK(ow_marshal2_cached_object(buffer, bitbuffer, (ow_DataStructure*)object->destination));
   SUCCESS_CHECK(ow_byte_buffer_append_byte(buffer, object->operationType));
   SUCCESS_CHECK(ow_marshal2_long(buffer, bitbuffer, object->timeout));
   SUCCESS_CHECK(ow_marshal2_DataStructure_array(buffer, bitbuffer, object->brokerPath));
   
	return APR_SUCCESS;
}

apr_status_t ow_unmarshal_DestinationInfo(ow_byte_array *buffer, ow_bit_buffer *bitbuffer, ow_DestinationInfo *object, apr_pool_t *pool)
{
   ow_unmarshal_BaseCommand(buffer, bitbuffer, (ow_BaseCommand*)object, pool);   
   SUCCESS_CHECK(ow_unmarshal_cached_object(buffer, bitbuffer, (ow_DataStructure**)&object->connectionId, pool));
   SUCCESS_CHECK(ow_unmarshal_cached_object(buffer, bitbuffer, (ow_DataStructure**)&object->destination, pool));
   SUCCESS_CHECK(ow_byte_array_read_byte(buffer, &object->operationType));
   SUCCESS_CHECK(ow_unmarshal_long(buffer, bitbuffer, &object->timeout, pool));
   SUCCESS_CHECK(ow_unmarshal_DataStructure_array(buffer, bitbuffer, &object->brokerPath, pool));
   
	return APR_SUCCESS;
}

ow_boolean ow_is_a_ShutdownInfo(ow_DataStructure *object) {
   if( object == 0 )
      return 0;
      
   switch(object->structType) {
   case OW_SHUTDOWNINFO_TYPE:
      return 1;
   }
   return 0;
}


ow_ShutdownInfo *ow_ShutdownInfo_create(apr_pool_t *pool) 
{
   ow_ShutdownInfo *value = apr_pcalloc(pool,sizeof(ow_ShutdownInfo));
   if( value!=0 ) {
      ((ow_DataStructure*)value)->structType = OW_SHUTDOWNINFO_TYPE;
   }
   return value;
}


apr_status_t ow_marshal1_ShutdownInfo(ow_bit_buffer *buffer, ow_ShutdownInfo *object)
{
   ow_marshal1_BaseCommand(buffer, (ow_BaseCommand*)object);
   
	return APR_SUCCESS;
}
apr_status_t ow_marshal2_ShutdownInfo(ow_byte_buffer *buffer, ow_bit_buffer *bitbuffer, ow_ShutdownInfo *object)
{
   ow_marshal2_BaseCommand(buffer, bitbuffer, (ow_BaseCommand*)object);   
   
	return APR_SUCCESS;
}

apr_status_t ow_unmarshal_ShutdownInfo(ow_byte_array *buffer, ow_bit_buffer *bitbuffer, ow_ShutdownInfo *object, apr_pool_t *pool)
{
   ow_unmarshal_BaseCommand(buffer, bitbuffer, (ow_BaseCommand*)object, pool);   
   
	return APR_SUCCESS;
}

ow_boolean ow_is_a_DataResponse(ow_DataStructure *object) {
   if( object == 0 )
      return 0;
      
   switch(object->structType) {
   case OW_DATARESPONSE_TYPE:
      return 1;
   }
   return 0;
}


ow_DataResponse *ow_DataResponse_create(apr_pool_t *pool) 
{
   ow_DataResponse *value = apr_pcalloc(pool,sizeof(ow_DataResponse));
   if( value!=0 ) {
      ((ow_DataStructure*)value)->structType = OW_DATARESPONSE_TYPE;
   }
   return value;
}


apr_status_t ow_marshal1_DataResponse(ow_bit_buffer *buffer, ow_DataResponse *object)
{
   ow_marshal1_Response(buffer, (ow_Response*)object);
   SUCCESS_CHECK(ow_marshal1_nested_object(buffer, (ow_DataStructure*)object->data));
   
	return APR_SUCCESS;
}
apr_status_t ow_marshal2_DataResponse(ow_byte_buffer *buffer, ow_bit_buffer *bitbuffer, ow_DataResponse *object)
{
   ow_marshal2_Response(buffer, bitbuffer, (ow_Response*)object);   
   SUCCESS_CHECK(ow_marshal2_nested_object(buffer, bitbuffer, (ow_DataStructure*)object->data));
   
	return APR_SUCCESS;
}

apr_status_t ow_unmarshal_DataResponse(ow_byte_array *buffer, ow_bit_buffer *bitbuffer, ow_DataResponse *object, apr_pool_t *pool)
{
   ow_unmarshal_Response(buffer, bitbuffer, (ow_Response*)object, pool);   
   SUCCESS_CHECK(ow_unmarshal_nested_object(buffer, bitbuffer, (ow_DataStructure**)&object->data, pool));
   
	return APR_SUCCESS;
}

ow_boolean ow_is_a_ConnectionControl(ow_DataStructure *object) {
   if( object == 0 )
      return 0;
      
   switch(object->structType) {
   case OW_CONNECTIONCONTROL_TYPE:
      return 1;
   }
   return 0;
}


ow_ConnectionControl *ow_ConnectionControl_create(apr_pool_t *pool) 
{
   ow_ConnectionControl *value = apr_pcalloc(pool,sizeof(ow_ConnectionControl));
   if( value!=0 ) {
      ((ow_DataStructure*)value)->structType = OW_CONNECTIONCONTROL_TYPE;
   }
   return value;
}


apr_status_t ow_marshal1_ConnectionControl(ow_bit_buffer *buffer, ow_ConnectionControl *object)
{
   ow_marshal1_BaseCommand(buffer, (ow_BaseCommand*)object);
   ow_bit_buffer_append(buffer, object->close);
   ow_bit_buffer_append(buffer, object->exit);
   ow_bit_buffer_append(buffer, object->faultTolerant);
   ow_bit_buffer_append(buffer, object->resume);
   ow_bit_buffer_append(buffer, object->suspend);
   
	return APR_SUCCESS;
}
apr_status_t ow_marshal2_ConnectionControl(ow_byte_buffer *buffer, ow_bit_buffer *bitbuffer, ow_ConnectionControl *object)
{
   ow_marshal2_BaseCommand(buffer, bitbuffer, (ow_BaseCommand*)object);   
   ow_bit_buffer_read(bitbuffer);
   ow_bit_buffer_read(bitbuffer);
   ow_bit_buffer_read(bitbuffer);
   ow_bit_buffer_read(bitbuffer);
   ow_bit_buffer_read(bitbuffer);
   
	return APR_SUCCESS;
}

apr_status_t ow_unmarshal_ConnectionControl(ow_byte_array *buffer, ow_bit_buffer *bitbuffer, ow_ConnectionControl *object, apr_pool_t *pool)
{
   ow_unmarshal_BaseCommand(buffer, bitbuffer, (ow_BaseCommand*)object, pool);   
   object->close = ow_bit_buffer_read(bitbuffer);
   object->exit = ow_bit_buffer_read(bitbuffer);
   object->faultTolerant = ow_bit_buffer_read(bitbuffer);
   object->resume = ow_bit_buffer_read(bitbuffer);
   object->suspend = ow_bit_buffer_read(bitbuffer);
   
	return APR_SUCCESS;
}

ow_boolean ow_is_a_KeepAliveInfo(ow_DataStructure *object) {
   if( object == 0 )
      return 0;
      
   switch(object->structType) {
   case OW_KEEPALIVEINFO_TYPE:
      return 1;
   }
   return 0;
}


ow_KeepAliveInfo *ow_KeepAliveInfo_create(apr_pool_t *pool) 
{
   ow_KeepAliveInfo *value = apr_pcalloc(pool,sizeof(ow_KeepAliveInfo));
   if( value!=0 ) {
      ((ow_DataStructure*)value)->structType = OW_KEEPALIVEINFO_TYPE;
   }
   return value;
}


apr_status_t ow_marshal1_KeepAliveInfo(ow_bit_buffer *buffer, ow_KeepAliveInfo *object)
{
   ow_marshal1_BaseCommand(buffer, (ow_BaseCommand*)object);
   
	return APR_SUCCESS;
}
apr_status_t ow_marshal2_KeepAliveInfo(ow_byte_buffer *buffer, ow_bit_buffer *bitbuffer, ow_KeepAliveInfo *object)
{
   ow_marshal2_BaseCommand(buffer, bitbuffer, (ow_BaseCommand*)object);   
   
	return APR_SUCCESS;
}

apr_status_t ow_unmarshal_KeepAliveInfo(ow_byte_array *buffer, ow_bit_buffer *bitbuffer, ow_KeepAliveInfo *object, apr_pool_t *pool)
{
   ow_unmarshal_BaseCommand(buffer, bitbuffer, (ow_BaseCommand*)object, pool);   
   
	return APR_SUCCESS;
}

ow_boolean ow_is_a_Message(ow_DataStructure *object) {
   if( object == 0 )
      return 0;
      
   switch(object->structType) {
   case OW_ACTIVEMQOBJECTMESSAGE_TYPE:
   case OW_ACTIVEMQSTREAMMESSAGE_TYPE:
   case OW_ACTIVEMQTEXTMESSAGE_TYPE:
   case OW_ACTIVEMQBYTESMESSAGE_TYPE:
   case OW_ACTIVEMQMAPMESSAGE_TYPE:
   case OW_ACTIVEMQMESSAGE_TYPE:
      return 1;
   }
   return 0;
}


apr_status_t ow_marshal1_Message(ow_bit_buffer *buffer, ow_Message *object)
{
   ow_marshal1_BaseCommand(buffer, (ow_BaseCommand*)object);
   SUCCESS_CHECK(ow_marshal1_cached_object(buffer, (ow_DataStructure*)object->producerId));
   SUCCESS_CHECK(ow_marshal1_cached_object(buffer, (ow_DataStructure*)object->destination));
   SUCCESS_CHECK(ow_marshal1_cached_object(buffer, (ow_DataStructure*)object->transactionId));
   SUCCESS_CHECK(ow_marshal1_cached_object(buffer, (ow_DataStructure*)object->originalDestination));
   SUCCESS_CHECK(ow_marshal1_nested_object(buffer, (ow_DataStructure*)object->messageId));
   SUCCESS_CHECK(ow_marshal1_cached_object(buffer, (ow_DataStructure*)object->originalTransactionId));
   ow_marshal1_string(buffer, object->groupID);
   
   ow_marshal1_string(buffer, object->correlationId);
   ow_bit_buffer_append(buffer, object->persistent);
   ow_marshal1_long(buffer, object->expiration);
   
   SUCCESS_CHECK(ow_marshal1_nested_object(buffer, (ow_DataStructure*)object->replyTo));
   ow_marshal1_long(buffer, object->timestamp);
   ow_marshal1_string(buffer, object->type);
   SUCCESS_CHECK(ow_marshal1_nested_object(buffer, (ow_DataStructure*)object->content));
   SUCCESS_CHECK(ow_marshal1_nested_object(buffer, (ow_DataStructure*)object->marshalledProperties));
   SUCCESS_CHECK(ow_marshal1_nested_object(buffer, (ow_DataStructure*)object->dataStructure));
   SUCCESS_CHECK(ow_marshal1_cached_object(buffer, (ow_DataStructure*)object->targetConsumerId));
   ow_bit_buffer_append(buffer, object->compressed);
   
   SUCCESS_CHECK(ow_marshal1_DataStructure_array(buffer, object->brokerPath));
   ow_marshal1_long(buffer, object->arrival);
   ow_marshal1_string(buffer, object->userID);
   ow_bit_buffer_append(buffer, object->recievedByDFBridge);
   
	return APR_SUCCESS;
}
apr_status_t ow_marshal2_Message(ow_byte_buffer *buffer, ow_bit_buffer *bitbuffer, ow_Message *object)
{
   ow_marshal2_BaseCommand(buffer, bitbuffer, (ow_BaseCommand*)object);   
   SUCCESS_CHECK(ow_marshal2_cached_object(buffer, bitbuffer, (ow_DataStructure*)object->producerId));
   SUCCESS_CHECK(ow_marshal2_cached_object(buffer, bitbuffer, (ow_DataStructure*)object->destination));
   SUCCESS_CHECK(ow_marshal2_cached_object(buffer, bitbuffer, (ow_DataStructure*)object->transactionId));
   SUCCESS_CHECK(ow_marshal2_cached_object(buffer, bitbuffer, (ow_DataStructure*)object->originalDestination));
   SUCCESS_CHECK(ow_marshal2_nested_object(buffer, bitbuffer, (ow_DataStructure*)object->messageId));
   SUCCESS_CHECK(ow_marshal2_cached_object(buffer, bitbuffer, (ow_DataStructure*)object->originalTransactionId));
   SUCCESS_CHECK(ow_marshal2_string(buffer, bitbuffer, object->groupID));
   SUCCESS_CHECK(ow_byte_buffer_append_int(buffer, object->groupSequence));
   SUCCESS_CHECK(ow_marshal2_string(buffer, bitbuffer, object->correlationId));
   ow_bit_buffer_read(bitbuffer);
   SUCCESS_CHECK(ow_marshal2_long(buffer, bitbuffer, object->expiration));
   SUCCESS_CHECK(ow_byte_buffer_append_byte(buffer, object->priority));
   SUCCESS_CHECK(ow_marshal2_nested_object(buffer, bitbuffer, (ow_DataStructure*)object->replyTo));
   SUCCESS_CHECK(ow_marshal2_long(buffer, bitbuffer, object->timestamp));
   SUCCESS_CHECK(ow_marshal2_string(buffer, bitbuffer, object->type));
   SUCCESS_CHECK(ow_marshal2_nested_object(buffer, bitbuffer, (ow_DataStructure*)object->content));
   SUCCESS_CHECK(ow_marshal2_nested_object(buffer, bitbuffer, (ow_DataStructure*)object->marshalledProperties));
   SUCCESS_CHECK(ow_marshal2_nested_object(buffer, bitbuffer, (ow_DataStructure*)object->dataStructure));
   SUCCESS_CHECK(ow_marshal2_cached_object(buffer, bitbuffer, (ow_DataStructure*)object->targetConsumerId));
   ow_bit_buffer_read(bitbuffer);
   SUCCESS_CHECK(ow_byte_buffer_append_int(buffer, object->redeliveryCounter));
   SUCCESS_CHECK(ow_marshal2_DataStructure_array(buffer, bitbuffer, object->brokerPath));
   SUCCESS_CHECK(ow_marshal2_long(buffer, bitbuffer, object->arrival));
   SUCCESS_CHECK(ow_marshal2_string(buffer, bitbuffer, object->userID));
   ow_bit_buffer_read(bitbuffer);
   
	return APR_SUCCESS;
}

apr_status_t ow_unmarshal_Message(ow_byte_array *buffer, ow_bit_buffer *bitbuffer, ow_Message *object, apr_pool_t *pool)
{
   ow_unmarshal_BaseCommand(buffer, bitbuffer, (ow_BaseCommand*)object, pool);   
   SUCCESS_CHECK(ow_unmarshal_cached_object(buffer, bitbuffer, (ow_DataStructure**)&object->producerId, pool));
   SUCCESS_CHECK(ow_unmarshal_cached_object(buffer, bitbuffer, (ow_DataStructure**)&object->destination, pool));
   SUCCESS_CHECK(ow_unmarshal_cached_object(buffer, bitbuffer, (ow_DataStructure**)&object->transactionId, pool));
   SUCCESS_CHECK(ow_unmarshal_cached_object(buffer, bitbuffer, (ow_DataStructure**)&object->originalDestination, pool));
   SUCCESS_CHECK(ow_unmarshal_nested_object(buffer, bitbuffer, (ow_DataStructure**)&object->messageId, pool));
   SUCCESS_CHECK(ow_unmarshal_cached_object(buffer, bitbuffer, (ow_DataStructure**)&object->originalTransactionId, pool));
   SUCCESS_CHECK(ow_unmarshal_string(buffer, bitbuffer, &object->groupID, pool));
   SUCCESS_CHECK(ow_byte_array_read_int(buffer, &object->groupSequence));
   SUCCESS_CHECK(ow_unmarshal_string(buffer, bitbuffer, &object->correlationId, pool));
   object->persistent = ow_bit_buffer_read(bitbuffer);
   SUCCESS_CHECK(ow_unmarshal_long(buffer, bitbuffer, &object->expiration, pool));
   SUCCESS_CHECK(ow_byte_array_read_byte(buffer, &object->priority));
   SUCCESS_CHECK(ow_unmarshal_nested_object(buffer, bitbuffer, (ow_DataStructure**)&object->replyTo, pool));
   SUCCESS_CHECK(ow_unmarshal_long(buffer, bitbuffer, &object->timestamp, pool));
   SUCCESS_CHECK(ow_unmarshal_string(buffer, bitbuffer, &object->type, pool));
   SUCCESS_CHECK(ow_unmarshal_nested_object(buffer, bitbuffer, (ow_DataStructure**)&object->content, pool));
   SUCCESS_CHECK(ow_unmarshal_nested_object(buffer, bitbuffer, (ow_DataStructure**)&object->marshalledProperties, pool));
   SUCCESS_CHECK(ow_unmarshal_nested_object(buffer, bitbuffer, (ow_DataStructure**)&object->dataStructure, pool));
   SUCCESS_CHECK(ow_unmarshal_cached_object(buffer, bitbuffer, (ow_DataStructure**)&object->targetConsumerId, pool));
   object->compressed = ow_bit_buffer_read(bitbuffer);
   SUCCESS_CHECK(ow_byte_array_read_int(buffer, &object->redeliveryCounter));
   SUCCESS_CHECK(ow_unmarshal_DataStructure_array(buffer, bitbuffer, &object->brokerPath, pool));
   SUCCESS_CHECK(ow_unmarshal_long(buffer, bitbuffer, &object->arrival, pool));
   SUCCESS_CHECK(ow_unmarshal_string(buffer, bitbuffer, &object->userID, pool));
   object->recievedByDFBridge = ow_bit_buffer_read(bitbuffer);
   
	return APR_SUCCESS;
}

ow_boolean ow_is_a_BaseCommand(ow_DataStructure *object) {
   if( object == 0 )
      return 0;
      
   switch(object->structType) {
   case OW_INTEGERRESPONSE_TYPE:
   case OW_ACTIVEMQOBJECTMESSAGE_TYPE:
   case OW_CONNECTIONINFO_TYPE:
   case OW_PRODUCERINFO_TYPE:
   case OW_MESSAGEDISPATCHNOTIFICATION_TYPE:
   case OW_SESSIONINFO_TYPE:
   case OW_TRANSACTIONINFO_TYPE:
   case OW_ACTIVEMQSTREAMMESSAGE_TYPE:
   case OW_MESSAGEACK_TYPE:
   case OW_REMOVESUBSCRIPTIONINFO_TYPE:
   case OW_DATAARRAYRESPONSE_TYPE:
   case OW_RESPONSE_TYPE:
   case OW_CONNECTIONERROR_TYPE:
   case OW_CONSUMERINFO_TYPE:
   case OW_ACTIVEMQTEXTMESSAGE_TYPE:
   case OW_CONTROLCOMMAND_TYPE:
   case OW_ACTIVEMQBYTESMESSAGE_TYPE:
   case OW_REPLAYCOMMAND_TYPE:
   case OW_BROKERINFO_TYPE:
   case OW_DESTINATIONINFO_TYPE:
   case OW_SHUTDOWNINFO_TYPE:
   case OW_DATARESPONSE_TYPE:
   case OW_CONNECTIONCONTROL_TYPE:
   case OW_KEEPALIVEINFO_TYPE:
   case OW_FLUSHCOMMAND_TYPE:
   case OW_CONSUMERCONTROL_TYPE:
   case OW_MESSAGEDISPATCH_TYPE:
   case OW_ACTIVEMQMAPMESSAGE_TYPE:
   case OW_ACTIVEMQMESSAGE_TYPE:
   case OW_REMOVEINFO_TYPE:
   case OW_EXCEPTIONRESPONSE_TYPE:
      return 1;
   }
   return 0;
}


apr_status_t ow_marshal1_BaseCommand(ow_bit_buffer *buffer, ow_BaseCommand *object)
{
   ow_marshal1_DataStructure(buffer, (ow_DataStructure*)object);
   
   ow_bit_buffer_append(buffer, object->responseRequired);
   
	return APR_SUCCESS;
}
apr_status_t ow_marshal2_BaseCommand(ow_byte_buffer *buffer, ow_bit_buffer *bitbuffer, ow_BaseCommand *object)
{
   ow_marshal2_DataStructure(buffer, bitbuffer, (ow_DataStructure*)object);   
   SUCCESS_CHECK(ow_byte_buffer_append_int(buffer, object->commandId));
   ow_bit_buffer_read(bitbuffer);
   
	return APR_SUCCESS;
}

apr_status_t ow_unmarshal_BaseCommand(ow_byte_array *buffer, ow_bit_buffer *bitbuffer, ow_BaseCommand *object, apr_pool_t *pool)
{
   ow_unmarshal_DataStructure(buffer, bitbuffer, (ow_DataStructure*)object, pool);   
   SUCCESS_CHECK(ow_byte_array_read_int(buffer, &object->commandId));
   object->responseRequired = ow_bit_buffer_read(bitbuffer);
   
	return APR_SUCCESS;
}

ow_boolean ow_is_a_FlushCommand(ow_DataStructure *object) {
   if( object == 0 )
      return 0;
      
   switch(object->structType) {
   case OW_FLUSHCOMMAND_TYPE:
      return 1;
   }
   return 0;
}


ow_FlushCommand *ow_FlushCommand_create(apr_pool_t *pool) 
{
   ow_FlushCommand *value = apr_pcalloc(pool,sizeof(ow_FlushCommand));
   if( value!=0 ) {
      ((ow_DataStructure*)value)->structType = OW_FLUSHCOMMAND_TYPE;
   }
   return value;
}


apr_status_t ow_marshal1_FlushCommand(ow_bit_buffer *buffer, ow_FlushCommand *object)
{
   ow_marshal1_BaseCommand(buffer, (ow_BaseCommand*)object);
   
	return APR_SUCCESS;
}
apr_status_t ow_marshal2_FlushCommand(ow_byte_buffer *buffer, ow_bit_buffer *bitbuffer, ow_FlushCommand *object)
{
   ow_marshal2_BaseCommand(buffer, bitbuffer, (ow_BaseCommand*)object);   
   
	return APR_SUCCESS;
}

apr_status_t ow_unmarshal_FlushCommand(ow_byte_array *buffer, ow_bit_buffer *bitbuffer, ow_FlushCommand *object, apr_pool_t *pool)
{
   ow_unmarshal_BaseCommand(buffer, bitbuffer, (ow_BaseCommand*)object, pool);   
   
	return APR_SUCCESS;
}

ow_boolean ow_is_a_ConsumerControl(ow_DataStructure *object) {
   if( object == 0 )
      return 0;
      
   switch(object->structType) {
   case OW_CONSUMERCONTROL_TYPE:
      return 1;
   }
   return 0;
}


ow_ConsumerControl *ow_ConsumerControl_create(apr_pool_t *pool) 
{
   ow_ConsumerControl *value = apr_pcalloc(pool,sizeof(ow_ConsumerControl));
   if( value!=0 ) {
      ((ow_DataStructure*)value)->structType = OW_CONSUMERCONTROL_TYPE;
   }
   return value;
}


apr_status_t ow_marshal1_ConsumerControl(ow_bit_buffer *buffer, ow_ConsumerControl *object)
{
   ow_marshal1_BaseCommand(buffer, (ow_BaseCommand*)object);
   ow_bit_buffer_append(buffer, object->close);
   SUCCESS_CHECK(ow_marshal1_nested_object(buffer, (ow_DataStructure*)object->consumerId));
   
   
	return APR_SUCCESS;
}
apr_status_t ow_marshal2_ConsumerControl(ow_byte_buffer *buffer, ow_bit_buffer *bitbuffer, ow_ConsumerControl *object)
{
   ow_marshal2_BaseCommand(buffer, bitbuffer, (ow_BaseCommand*)object);   
   ow_bit_buffer_read(bitbuffer);
   SUCCESS_CHECK(ow_marshal2_nested_object(buffer, bitbuffer, (ow_DataStructure*)object->consumerId));
   SUCCESS_CHECK(ow_byte_buffer_append_int(buffer, object->prefetch));
   
	return APR_SUCCESS;
}

apr_status_t ow_unmarshal_ConsumerControl(ow_byte_array *buffer, ow_bit_buffer *bitbuffer, ow_ConsumerControl *object, apr_pool_t *pool)
{
   ow_unmarshal_BaseCommand(buffer, bitbuffer, (ow_BaseCommand*)object, pool);   
   object->close = ow_bit_buffer_read(bitbuffer);
   SUCCESS_CHECK(ow_unmarshal_nested_object(buffer, bitbuffer, (ow_DataStructure**)&object->consumerId, pool));
   SUCCESS_CHECK(ow_byte_array_read_int(buffer, &object->prefetch));
   
	return APR_SUCCESS;
}

ow_boolean ow_is_a_JournalTopicAck(ow_DataStructure *object) {
   if( object == 0 )
      return 0;
      
   switch(object->structType) {
   case OW_JOURNALTOPICACK_TYPE:
      return 1;
   }
   return 0;
}


ow_JournalTopicAck *ow_JournalTopicAck_create(apr_pool_t *pool) 
{
   ow_JournalTopicAck *value = apr_pcalloc(pool,sizeof(ow_JournalTopicAck));
   if( value!=0 ) {
      ((ow_DataStructure*)value)->structType = OW_JOURNALTOPICACK_TYPE;
   }
   return value;
}


apr_status_t ow_marshal1_JournalTopicAck(ow_bit_buffer *buffer, ow_JournalTopicAck *object)
{
   ow_marshal1_DataStructure(buffer, (ow_DataStructure*)object);
   SUCCESS_CHECK(ow_marshal1_nested_object(buffer, (ow_DataStructure*)object->destination));
   SUCCESS_CHECK(ow_marshal1_nested_object(buffer, (ow_DataStructure*)object->messageId));
   ow_marshal1_long(buffer, object->messageSequenceId);
   ow_marshal1_string(buffer, object->subscritionName);
   ow_marshal1_string(buffer, object->clientId);
   SUCCESS_CHECK(ow_marshal1_nested_object(buffer, (ow_DataStructure*)object->transactionId));
   
	return APR_SUCCESS;
}
apr_status_t ow_marshal2_JournalTopicAck(ow_byte_buffer *buffer, ow_bit_buffer *bitbuffer, ow_JournalTopicAck *object)
{
   ow_marshal2_DataStructure(buffer, bitbuffer, (ow_DataStructure*)object);   
   SUCCESS_CHECK(ow_marshal2_nested_object(buffer, bitbuffer, (ow_DataStructure*)object->destination));
   SUCCESS_CHECK(ow_marshal2_nested_object(buffer, bitbuffer, (ow_DataStructure*)object->messageId));
   SUCCESS_CHECK(ow_marshal2_long(buffer, bitbuffer, object->messageSequenceId));
   SUCCESS_CHECK(ow_marshal2_string(buffer, bitbuffer, object->subscritionName));
   SUCCESS_CHECK(ow_marshal2_string(buffer, bitbuffer, object->clientId));
   SUCCESS_CHECK(ow_marshal2_nested_object(buffer, bitbuffer, (ow_DataStructure*)object->transactionId));
   
	return APR_SUCCESS;
}

apr_status_t ow_unmarshal_JournalTopicAck(ow_byte_array *buffer, ow_bit_buffer *bitbuffer, ow_JournalTopicAck *object, apr_pool_t *pool)
{
   ow_unmarshal_DataStructure(buffer, bitbuffer, (ow_DataStructure*)object, pool);   
   SUCCESS_CHECK(ow_unmarshal_nested_object(buffer, bitbuffer, (ow_DataStructure**)&object->destination, pool));
   SUCCESS_CHECK(ow_unmarshal_nested_object(buffer, bitbuffer, (ow_DataStructure**)&object->messageId, pool));
   SUCCESS_CHECK(ow_unmarshal_long(buffer, bitbuffer, &object->messageSequenceId, pool));
   SUCCESS_CHECK(ow_unmarshal_string(buffer, bitbuffer, &object->subscritionName, pool));
   SUCCESS_CHECK(ow_unmarshal_string(buffer, bitbuffer, &object->clientId, pool));
   SUCCESS_CHECK(ow_unmarshal_nested_object(buffer, bitbuffer, (ow_DataStructure**)&object->transactionId, pool));
   
	return APR_SUCCESS;
}

ow_boolean ow_is_a_BrokerId(ow_DataStructure *object) {
   if( object == 0 )
      return 0;
      
   switch(object->structType) {
   case OW_BROKERID_TYPE:
      return 1;
   }
   return 0;
}


ow_BrokerId *ow_BrokerId_create(apr_pool_t *pool) 
{
   ow_BrokerId *value = apr_pcalloc(pool,sizeof(ow_BrokerId));
   if( value!=0 ) {
      ((ow_DataStructure*)value)->structType = OW_BROKERID_TYPE;
   }
   return value;
}


apr_status_t ow_marshal1_BrokerId(ow_bit_buffer *buffer, ow_BrokerId *object)
{
   ow_marshal1_DataStructure(buffer, (ow_DataStructure*)object);
   ow_marshal1_string(buffer, object->value);
   
	return APR_SUCCESS;
}
apr_status_t ow_marshal2_BrokerId(ow_byte_buffer *buffer, ow_bit_buffer *bitbuffer, ow_BrokerId *object)
{
   ow_marshal2_DataStructure(buffer, bitbuffer, (ow_DataStructure*)object);   
   SUCCESS_CHECK(ow_marshal2_string(buffer, bitbuffer, object->value));
   
	return APR_SUCCESS;
}

apr_status_t ow_unmarshal_BrokerId(ow_byte_array *buffer, ow_bit_buffer *bitbuffer, ow_BrokerId *object, apr_pool_t *pool)
{
   ow_unmarshal_DataStructure(buffer, bitbuffer, (ow_DataStructure*)object, pool);   
   SUCCESS_CHECK(ow_unmarshal_string(buffer, bitbuffer, &object->value, pool));
   
	return APR_SUCCESS;
}

ow_boolean ow_is_a_MessageDispatch(ow_DataStructure *object) {
   if( object == 0 )
      return 0;
      
   switch(object->structType) {
   case OW_MESSAGEDISPATCH_TYPE:
      return 1;
   }
   return 0;
}


ow_MessageDispatch *ow_MessageDispatch_create(apr_pool_t *pool) 
{
   ow_MessageDispatch *value = apr_pcalloc(pool,sizeof(ow_MessageDispatch));
   if( value!=0 ) {
      ((ow_DataStructure*)value)->structType = OW_MESSAGEDISPATCH_TYPE;
   }
   return value;
}


apr_status_t ow_marshal1_MessageDispatch(ow_bit_buffer *buffer, ow_MessageDispatch *object)
{
   ow_marshal1_BaseCommand(buffer, (ow_BaseCommand*)object);
   SUCCESS_CHECK(ow_marshal1_cached_object(buffer, (ow_DataStructure*)object->consumerId));
   SUCCESS_CHECK(ow_marshal1_cached_object(buffer, (ow_DataStructure*)object->destination));
   SUCCESS_CHECK(ow_marshal1_nested_object(buffer, (ow_DataStructure*)object->message));
   
   
	return APR_SUCCESS;
}
apr_status_t ow_marshal2_MessageDispatch(ow_byte_buffer *buffer, ow_bit_buffer *bitbuffer, ow_MessageDispatch *object)
{
   ow_marshal2_BaseCommand(buffer, bitbuffer, (ow_BaseCommand*)object);   
   SUCCESS_CHECK(ow_marshal2_cached_object(buffer, bitbuffer, (ow_DataStructure*)object->consumerId));
   SUCCESS_CHECK(ow_marshal2_cached_object(buffer, bitbuffer, (ow_DataStructure*)object->destination));
   SUCCESS_CHECK(ow_marshal2_nested_object(buffer, bitbuffer, (ow_DataStructure*)object->message));
   SUCCESS_CHECK(ow_byte_buffer_append_int(buffer, object->redeliveryCounter));
   
	return APR_SUCCESS;
}

apr_status_t ow_unmarshal_MessageDispatch(ow_byte_array *buffer, ow_bit_buffer *bitbuffer, ow_MessageDispatch *object, apr_pool_t *pool)
{
   ow_unmarshal_BaseCommand(buffer, bitbuffer, (ow_BaseCommand*)object, pool);   
   SUCCESS_CHECK(ow_unmarshal_cached_object(buffer, bitbuffer, (ow_DataStructure**)&object->consumerId, pool));
   SUCCESS_CHECK(ow_unmarshal_cached_object(buffer, bitbuffer, (ow_DataStructure**)&object->destination, pool));
   SUCCESS_CHECK(ow_unmarshal_nested_object(buffer, bitbuffer, (ow_DataStructure**)&object->message, pool));
   SUCCESS_CHECK(ow_byte_array_read_int(buffer, &object->redeliveryCounter));
   
	return APR_SUCCESS;
}

ow_boolean ow_is_a_ActiveMQMapMessage(ow_DataStructure *object) {
   if( object == 0 )
      return 0;
      
   switch(object->structType) {
   case OW_ACTIVEMQMAPMESSAGE_TYPE:
      return 1;
   }
   return 0;
}


ow_ActiveMQMapMessage *ow_ActiveMQMapMessage_create(apr_pool_t *pool) 
{
   ow_ActiveMQMapMessage *value = apr_pcalloc(pool,sizeof(ow_ActiveMQMapMessage));
   if( value!=0 ) {
      ((ow_DataStructure*)value)->structType = OW_ACTIVEMQMAPMESSAGE_TYPE;
   }
   return value;
}


apr_status_t ow_marshal1_ActiveMQMapMessage(ow_bit_buffer *buffer, ow_ActiveMQMapMessage *object)
{
   ow_marshal1_ActiveMQMessage(buffer, (ow_ActiveMQMessage*)object);
   
	return APR_SUCCESS;
}
apr_status_t ow_marshal2_ActiveMQMapMessage(ow_byte_buffer *buffer, ow_bit_buffer *bitbuffer, ow_ActiveMQMapMessage *object)
{
   ow_marshal2_ActiveMQMessage(buffer, bitbuffer, (ow_ActiveMQMessage*)object);   
   
	return APR_SUCCESS;
}

apr_status_t ow_unmarshal_ActiveMQMapMessage(ow_byte_array *buffer, ow_bit_buffer *bitbuffer, ow_ActiveMQMapMessage *object, apr_pool_t *pool)
{
   ow_unmarshal_ActiveMQMessage(buffer, bitbuffer, (ow_ActiveMQMessage*)object, pool);   
   
	return APR_SUCCESS;
}

ow_boolean ow_is_a_ActiveMQMessage(ow_DataStructure *object) {
   if( object == 0 )
      return 0;
      
   switch(object->structType) {
   case OW_ACTIVEMQOBJECTMESSAGE_TYPE:
   case OW_ACTIVEMQSTREAMMESSAGE_TYPE:
   case OW_ACTIVEMQTEXTMESSAGE_TYPE:
   case OW_ACTIVEMQBYTESMESSAGE_TYPE:
   case OW_ACTIVEMQMAPMESSAGE_TYPE:
   case OW_ACTIVEMQMESSAGE_TYPE:
      return 1;
   }
   return 0;
}


ow_ActiveMQMessage *ow_ActiveMQMessage_create(apr_pool_t *pool) 
{
   ow_ActiveMQMessage *value = apr_pcalloc(pool,sizeof(ow_ActiveMQMessage));
   if( value!=0 ) {
      ((ow_DataStructure*)value)->structType = OW_ACTIVEMQMESSAGE_TYPE;
   }
   return value;
}


apr_status_t ow_marshal1_ActiveMQMessage(ow_bit_buffer *buffer, ow_ActiveMQMessage *object)
{
   ow_marshal1_Message(buffer, (ow_Message*)object);
   
	return APR_SUCCESS;
}
apr_status_t ow_marshal2_ActiveMQMessage(ow_byte_buffer *buffer, ow_bit_buffer *bitbuffer, ow_ActiveMQMessage *object)
{
   ow_marshal2_Message(buffer, bitbuffer, (ow_Message*)object);   
   
	return APR_SUCCESS;
}

apr_status_t ow_unmarshal_ActiveMQMessage(ow_byte_array *buffer, ow_bit_buffer *bitbuffer, ow_ActiveMQMessage *object, apr_pool_t *pool)
{
   ow_unmarshal_Message(buffer, bitbuffer, (ow_Message*)object, pool);   
   
	return APR_SUCCESS;
}

ow_boolean ow_is_a_RemoveInfo(ow_DataStructure *object) {
   if( object == 0 )
      return 0;
      
   switch(object->structType) {
   case OW_REMOVEINFO_TYPE:
      return 1;
   }
   return 0;
}


ow_RemoveInfo *ow_RemoveInfo_create(apr_pool_t *pool) 
{
   ow_RemoveInfo *value = apr_pcalloc(pool,sizeof(ow_RemoveInfo));
   if( value!=0 ) {
      ((ow_DataStructure*)value)->structType = OW_REMOVEINFO_TYPE;
   }
   return value;
}


apr_status_t ow_marshal1_RemoveInfo(ow_bit_buffer *buffer, ow_RemoveInfo *object)
{
   ow_marshal1_BaseCommand(buffer, (ow_BaseCommand*)object);
   SUCCESS_CHECK(ow_marshal1_cached_object(buffer, (ow_DataStructure*)object->objectId));
   
	return APR_SUCCESS;
}
apr_status_t ow_marshal2_RemoveInfo(ow_byte_buffer *buffer, ow_bit_buffer *bitbuffer, ow_RemoveInfo *object)
{
   ow_marshal2_BaseCommand(buffer, bitbuffer, (ow_BaseCommand*)object);   
   SUCCESS_CHECK(ow_marshal2_cached_object(buffer, bitbuffer, (ow_DataStructure*)object->objectId));
   
	return APR_SUCCESS;
}

apr_status_t ow_unmarshal_RemoveInfo(ow_byte_array *buffer, ow_bit_buffer *bitbuffer, ow_RemoveInfo *object, apr_pool_t *pool)
{
   ow_unmarshal_BaseCommand(buffer, bitbuffer, (ow_BaseCommand*)object, pool);   
   SUCCESS_CHECK(ow_unmarshal_cached_object(buffer, bitbuffer, (ow_DataStructure**)&object->objectId, pool));
   
	return APR_SUCCESS;
}

ow_boolean ow_is_a_ExceptionResponse(ow_DataStructure *object) {
   if( object == 0 )
      return 0;
      
   switch(object->structType) {
   case OW_EXCEPTIONRESPONSE_TYPE:
      return 1;
   }
   return 0;
}


ow_ExceptionResponse *ow_ExceptionResponse_create(apr_pool_t *pool) 
{
   ow_ExceptionResponse *value = apr_pcalloc(pool,sizeof(ow_ExceptionResponse));
   if( value!=0 ) {
      ((ow_DataStructure*)value)->structType = OW_EXCEPTIONRESPONSE_TYPE;
   }
   return value;
}


apr_status_t ow_marshal1_ExceptionResponse(ow_bit_buffer *buffer, ow_ExceptionResponse *object)
{
   ow_marshal1_Response(buffer, (ow_Response*)object);
   SUCCESS_CHECK(ow_marshal1_throwable(buffer, object->exception));
   
	return APR_SUCCESS;
}
apr_status_t ow_marshal2_ExceptionResponse(ow_byte_buffer *buffer, ow_bit_buffer *bitbuffer, ow_ExceptionResponse *object)
{
   ow_marshal2_Response(buffer, bitbuffer, (ow_Response*)object);   
   SUCCESS_CHECK(ow_marshal2_throwable(buffer, bitbuffer, object->exception));
   
	return APR_SUCCESS;
}

apr_status_t ow_unmarshal_ExceptionResponse(ow_byte_array *buffer, ow_bit_buffer *bitbuffer, ow_ExceptionResponse *object, apr_pool_t *pool)
{
   ow_unmarshal_Response(buffer, bitbuffer, (ow_Response*)object, pool);   
   SUCCESS_CHECK(ow_unmarshal_throwable(buffer, bitbuffer, &object->exception, pool));
   
	return APR_SUCCESS;
}

ow_DataStructure *ow_create_object(ow_byte type, apr_pool_t *pool)
{
   switch( type ) {

      case OW_LOCALTRANSACTIONID_TYPE: return (ow_DataStructure *)ow_LocalTransactionId_create(pool);
      case OW_PARTIALCOMMAND_TYPE: return (ow_DataStructure *)ow_PartialCommand_create(pool);
      case OW_INTEGERRESPONSE_TYPE: return (ow_DataStructure *)ow_IntegerResponse_create(pool);
      case OW_ACTIVEMQQUEUE_TYPE: return (ow_DataStructure *)ow_ActiveMQQueue_create(pool);
      case OW_ACTIVEMQOBJECTMESSAGE_TYPE: return (ow_DataStructure *)ow_ActiveMQObjectMessage_create(pool);
      case OW_CONNECTIONID_TYPE: return (ow_DataStructure *)ow_ConnectionId_create(pool);
      case OW_CONNECTIONINFO_TYPE: return (ow_DataStructure *)ow_ConnectionInfo_create(pool);
      case OW_PRODUCERINFO_TYPE: return (ow_DataStructure *)ow_ProducerInfo_create(pool);
      case OW_MESSAGEDISPATCHNOTIFICATION_TYPE: return (ow_DataStructure *)ow_MessageDispatchNotification_create(pool);
      case OW_SESSIONINFO_TYPE: return (ow_DataStructure *)ow_SessionInfo_create(pool);
      case OW_TRANSACTIONINFO_TYPE: return (ow_DataStructure *)ow_TransactionInfo_create(pool);
      case OW_ACTIVEMQSTREAMMESSAGE_TYPE: return (ow_DataStructure *)ow_ActiveMQStreamMessage_create(pool);
      case OW_MESSAGEACK_TYPE: return (ow_DataStructure *)ow_MessageAck_create(pool);
      case OW_PRODUCERID_TYPE: return (ow_DataStructure *)ow_ProducerId_create(pool);
      case OW_MESSAGEID_TYPE: return (ow_DataStructure *)ow_MessageId_create(pool);
      case OW_ACTIVEMQTEMPQUEUE_TYPE: return (ow_DataStructure *)ow_ActiveMQTempQueue_create(pool);
      case OW_REMOVESUBSCRIPTIONINFO_TYPE: return (ow_DataStructure *)ow_RemoveSubscriptionInfo_create(pool);
      case OW_SESSIONID_TYPE: return (ow_DataStructure *)ow_SessionId_create(pool);
      case OW_DATAARRAYRESPONSE_TYPE: return (ow_DataStructure *)ow_DataArrayResponse_create(pool);
      case OW_JOURNALQUEUEACK_TYPE: return (ow_DataStructure *)ow_JournalQueueAck_create(pool);
      case OW_RESPONSE_TYPE: return (ow_DataStructure *)ow_Response_create(pool);
      case OW_CONNECTIONERROR_TYPE: return (ow_DataStructure *)ow_ConnectionError_create(pool);
      case OW_CONSUMERINFO_TYPE: return (ow_DataStructure *)ow_ConsumerInfo_create(pool);
      case OW_XATRANSACTIONID_TYPE: return (ow_DataStructure *)ow_XATransactionId_create(pool);
      case OW_JOURNALTRACE_TYPE: return (ow_DataStructure *)ow_JournalTrace_create(pool);
      case OW_CONSUMERID_TYPE: return (ow_DataStructure *)ow_ConsumerId_create(pool);
      case OW_ACTIVEMQTEXTMESSAGE_TYPE: return (ow_DataStructure *)ow_ActiveMQTextMessage_create(pool);
      case OW_SUBSCRIPTIONINFO_TYPE: return (ow_DataStructure *)ow_SubscriptionInfo_create(pool);
      case OW_JOURNALTRANSACTION_TYPE: return (ow_DataStructure *)ow_JournalTransaction_create(pool);
      case OW_CONTROLCOMMAND_TYPE: return (ow_DataStructure *)ow_ControlCommand_create(pool);
      case OW_LASTPARTIALCOMMAND_TYPE: return (ow_DataStructure *)ow_LastPartialCommand_create(pool);
      case OW_NETWORKBRIDGEFILTER_TYPE: return (ow_DataStructure *)ow_NetworkBridgeFilter_create(pool);
      case OW_ACTIVEMQBYTESMESSAGE_TYPE: return (ow_DataStructure *)ow_ActiveMQBytesMessage_create(pool);
      case OW_WIREFORMATINFO_TYPE: return (ow_DataStructure *)ow_WireFormatInfo_create(pool);
      case OW_ACTIVEMQTEMPTOPIC_TYPE: return (ow_DataStructure *)ow_ActiveMQTempTopic_create(pool);
      case OW_DISCOVERYEVENT_TYPE: return (ow_DataStructure *)ow_DiscoveryEvent_create(pool);
      case OW_REPLAYCOMMAND_TYPE: return (ow_DataStructure *)ow_ReplayCommand_create(pool);
      case OW_ACTIVEMQTOPIC_TYPE: return (ow_DataStructure *)ow_ActiveMQTopic_create(pool);
      case OW_BROKERINFO_TYPE: return (ow_DataStructure *)ow_BrokerInfo_create(pool);
      case OW_DESTINATIONINFO_TYPE: return (ow_DataStructure *)ow_DestinationInfo_create(pool);
      case OW_SHUTDOWNINFO_TYPE: return (ow_DataStructure *)ow_ShutdownInfo_create(pool);
      case OW_DATARESPONSE_TYPE: return (ow_DataStructure *)ow_DataResponse_create(pool);
      case OW_CONNECTIONCONTROL_TYPE: return (ow_DataStructure *)ow_ConnectionControl_create(pool);
      case OW_KEEPALIVEINFO_TYPE: return (ow_DataStructure *)ow_KeepAliveInfo_create(pool);
      case OW_FLUSHCOMMAND_TYPE: return (ow_DataStructure *)ow_FlushCommand_create(pool);
      case OW_CONSUMERCONTROL_TYPE: return (ow_DataStructure *)ow_ConsumerControl_create(pool);
      case OW_JOURNALTOPICACK_TYPE: return (ow_DataStructure *)ow_JournalTopicAck_create(pool);
      case OW_BROKERID_TYPE: return (ow_DataStructure *)ow_BrokerId_create(pool);
      case OW_MESSAGEDISPATCH_TYPE: return (ow_DataStructure *)ow_MessageDispatch_create(pool);
      case OW_ACTIVEMQMAPMESSAGE_TYPE: return (ow_DataStructure *)ow_ActiveMQMapMessage_create(pool);
      case OW_ACTIVEMQMESSAGE_TYPE: return (ow_DataStructure *)ow_ActiveMQMessage_create(pool);
      case OW_REMOVEINFO_TYPE: return (ow_DataStructure *)ow_RemoveInfo_create(pool);
      case OW_EXCEPTIONRESPONSE_TYPE: return (ow_DataStructure *)ow_ExceptionResponse_create(pool);
   }
   return 0;
}

apr_status_t ow_marshal1_object(ow_bit_buffer *buffer, ow_DataStructure *object)
{
   switch( object->structType ) {

      case OW_LOCALTRANSACTIONID_TYPE: return ow_marshal1_LocalTransactionId(buffer, (ow_LocalTransactionId*)object);
      case OW_PARTIALCOMMAND_TYPE: return ow_marshal1_PartialCommand(buffer, (ow_PartialCommand*)object);
      case OW_INTEGERRESPONSE_TYPE: return ow_marshal1_IntegerResponse(buffer, (ow_IntegerResponse*)object);
      case OW_ACTIVEMQQUEUE_TYPE: return ow_marshal1_ActiveMQQueue(buffer, (ow_ActiveMQQueue*)object);
      case OW_ACTIVEMQOBJECTMESSAGE_TYPE: return ow_marshal1_ActiveMQObjectMessage(buffer, (ow_ActiveMQObjectMessage*)object);
      case OW_CONNECTIONID_TYPE: return ow_marshal1_ConnectionId(buffer, (ow_ConnectionId*)object);
      case OW_CONNECTIONINFO_TYPE: return ow_marshal1_ConnectionInfo(buffer, (ow_ConnectionInfo*)object);
      case OW_PRODUCERINFO_TYPE: return ow_marshal1_ProducerInfo(buffer, (ow_ProducerInfo*)object);
      case OW_MESSAGEDISPATCHNOTIFICATION_TYPE: return ow_marshal1_MessageDispatchNotification(buffer, (ow_MessageDispatchNotification*)object);
      case OW_SESSIONINFO_TYPE: return ow_marshal1_SessionInfo(buffer, (ow_SessionInfo*)object);
      case OW_TRANSACTIONINFO_TYPE: return ow_marshal1_TransactionInfo(buffer, (ow_TransactionInfo*)object);
      case OW_ACTIVEMQSTREAMMESSAGE_TYPE: return ow_marshal1_ActiveMQStreamMessage(buffer, (ow_ActiveMQStreamMessage*)object);
      case OW_MESSAGEACK_TYPE: return ow_marshal1_MessageAck(buffer, (ow_MessageAck*)object);
      case OW_PRODUCERID_TYPE: return ow_marshal1_ProducerId(buffer, (ow_ProducerId*)object);
      case OW_MESSAGEID_TYPE: return ow_marshal1_MessageId(buffer, (ow_MessageId*)object);
      case OW_ACTIVEMQTEMPQUEUE_TYPE: return ow_marshal1_ActiveMQTempQueue(buffer, (ow_ActiveMQTempQueue*)object);
      case OW_REMOVESUBSCRIPTIONINFO_TYPE: return ow_marshal1_RemoveSubscriptionInfo(buffer, (ow_RemoveSubscriptionInfo*)object);
      case OW_SESSIONID_TYPE: return ow_marshal1_SessionId(buffer, (ow_SessionId*)object);
      case OW_DATAARRAYRESPONSE_TYPE: return ow_marshal1_DataArrayResponse(buffer, (ow_DataArrayResponse*)object);
      case OW_JOURNALQUEUEACK_TYPE: return ow_marshal1_JournalQueueAck(buffer, (ow_JournalQueueAck*)object);
      case OW_RESPONSE_TYPE: return ow_marshal1_Response(buffer, (ow_Response*)object);
      case OW_CONNECTIONERROR_TYPE: return ow_marshal1_ConnectionError(buffer, (ow_ConnectionError*)object);
      case OW_CONSUMERINFO_TYPE: return ow_marshal1_ConsumerInfo(buffer, (ow_ConsumerInfo*)object);
      case OW_XATRANSACTIONID_TYPE: return ow_marshal1_XATransactionId(buffer, (ow_XATransactionId*)object);
      case OW_JOURNALTRACE_TYPE: return ow_marshal1_JournalTrace(buffer, (ow_JournalTrace*)object);
      case OW_CONSUMERID_TYPE: return ow_marshal1_ConsumerId(buffer, (ow_ConsumerId*)object);
      case OW_ACTIVEMQTEXTMESSAGE_TYPE: return ow_marshal1_ActiveMQTextMessage(buffer, (ow_ActiveMQTextMessage*)object);
      case OW_SUBSCRIPTIONINFO_TYPE: return ow_marshal1_SubscriptionInfo(buffer, (ow_SubscriptionInfo*)object);
      case OW_JOURNALTRANSACTION_TYPE: return ow_marshal1_JournalTransaction(buffer, (ow_JournalTransaction*)object);
      case OW_CONTROLCOMMAND_TYPE: return ow_marshal1_ControlCommand(buffer, (ow_ControlCommand*)object);
      case OW_LASTPARTIALCOMMAND_TYPE: return ow_marshal1_LastPartialCommand(buffer, (ow_LastPartialCommand*)object);
      case OW_NETWORKBRIDGEFILTER_TYPE: return ow_marshal1_NetworkBridgeFilter(buffer, (ow_NetworkBridgeFilter*)object);
      case OW_ACTIVEMQBYTESMESSAGE_TYPE: return ow_marshal1_ActiveMQBytesMessage(buffer, (ow_ActiveMQBytesMessage*)object);
      case OW_WIREFORMATINFO_TYPE: return ow_marshal1_WireFormatInfo(buffer, (ow_WireFormatInfo*)object);
      case OW_ACTIVEMQTEMPTOPIC_TYPE: return ow_marshal1_ActiveMQTempTopic(buffer, (ow_ActiveMQTempTopic*)object);
      case OW_DISCOVERYEVENT_TYPE: return ow_marshal1_DiscoveryEvent(buffer, (ow_DiscoveryEvent*)object);
      case OW_REPLAYCOMMAND_TYPE: return ow_marshal1_ReplayCommand(buffer, (ow_ReplayCommand*)object);
      case OW_ACTIVEMQTOPIC_TYPE: return ow_marshal1_ActiveMQTopic(buffer, (ow_ActiveMQTopic*)object);
      case OW_BROKERINFO_TYPE: return ow_marshal1_BrokerInfo(buffer, (ow_BrokerInfo*)object);
      case OW_DESTINATIONINFO_TYPE: return ow_marshal1_DestinationInfo(buffer, (ow_DestinationInfo*)object);
      case OW_SHUTDOWNINFO_TYPE: return ow_marshal1_ShutdownInfo(buffer, (ow_ShutdownInfo*)object);
      case OW_DATARESPONSE_TYPE: return ow_marshal1_DataResponse(buffer, (ow_DataResponse*)object);
      case OW_CONNECTIONCONTROL_TYPE: return ow_marshal1_ConnectionControl(buffer, (ow_ConnectionControl*)object);
      case OW_KEEPALIVEINFO_TYPE: return ow_marshal1_KeepAliveInfo(buffer, (ow_KeepAliveInfo*)object);
      case OW_FLUSHCOMMAND_TYPE: return ow_marshal1_FlushCommand(buffer, (ow_FlushCommand*)object);
      case OW_CONSUMERCONTROL_TYPE: return ow_marshal1_ConsumerControl(buffer, (ow_ConsumerControl*)object);
      case OW_JOURNALTOPICACK_TYPE: return ow_marshal1_JournalTopicAck(buffer, (ow_JournalTopicAck*)object);
      case OW_BROKERID_TYPE: return ow_marshal1_BrokerId(buffer, (ow_BrokerId*)object);
      case OW_MESSAGEDISPATCH_TYPE: return ow_marshal1_MessageDispatch(buffer, (ow_MessageDispatch*)object);
      case OW_ACTIVEMQMAPMESSAGE_TYPE: return ow_marshal1_ActiveMQMapMessage(buffer, (ow_ActiveMQMapMessage*)object);
      case OW_ACTIVEMQMESSAGE_TYPE: return ow_marshal1_ActiveMQMessage(buffer, (ow_ActiveMQMessage*)object);
      case OW_REMOVEINFO_TYPE: return ow_marshal1_RemoveInfo(buffer, (ow_RemoveInfo*)object);
      case OW_EXCEPTIONRESPONSE_TYPE: return ow_marshal1_ExceptionResponse(buffer, (ow_ExceptionResponse*)object);
   }
   return APR_EGENERAL;
}

apr_status_t ow_marshal2_object(ow_byte_buffer *buffer, ow_bit_buffer *bitbuffer, ow_DataStructure *object)
{
   switch( object->structType ) {

      case OW_LOCALTRANSACTIONID_TYPE: return ow_marshal2_LocalTransactionId(buffer, bitbuffer, (ow_LocalTransactionId*)object);
      case OW_PARTIALCOMMAND_TYPE: return ow_marshal2_PartialCommand(buffer, bitbuffer, (ow_PartialCommand*)object);
      case OW_INTEGERRESPONSE_TYPE: return ow_marshal2_IntegerResponse(buffer, bitbuffer, (ow_IntegerResponse*)object);
      case OW_ACTIVEMQQUEUE_TYPE: return ow_marshal2_ActiveMQQueue(buffer, bitbuffer, (ow_ActiveMQQueue*)object);
      case OW_ACTIVEMQOBJECTMESSAGE_TYPE: return ow_marshal2_ActiveMQObjectMessage(buffer, bitbuffer, (ow_ActiveMQObjectMessage*)object);
      case OW_CONNECTIONID_TYPE: return ow_marshal2_ConnectionId(buffer, bitbuffer, (ow_ConnectionId*)object);
      case OW_CONNECTIONINFO_TYPE: return ow_marshal2_ConnectionInfo(buffer, bitbuffer, (ow_ConnectionInfo*)object);
      case OW_PRODUCERINFO_TYPE: return ow_marshal2_ProducerInfo(buffer, bitbuffer, (ow_ProducerInfo*)object);
      case OW_MESSAGEDISPATCHNOTIFICATION_TYPE: return ow_marshal2_MessageDispatchNotification(buffer, bitbuffer, (ow_MessageDispatchNotification*)object);
      case OW_SESSIONINFO_TYPE: return ow_marshal2_SessionInfo(buffer, bitbuffer, (ow_SessionInfo*)object);
      case OW_TRANSACTIONINFO_TYPE: return ow_marshal2_TransactionInfo(buffer, bitbuffer, (ow_TransactionInfo*)object);
      case OW_ACTIVEMQSTREAMMESSAGE_TYPE: return ow_marshal2_ActiveMQStreamMessage(buffer, bitbuffer, (ow_ActiveMQStreamMessage*)object);
      case OW_MESSAGEACK_TYPE: return ow_marshal2_MessageAck(buffer, bitbuffer, (ow_MessageAck*)object);
      case OW_PRODUCERID_TYPE: return ow_marshal2_ProducerId(buffer, bitbuffer, (ow_ProducerId*)object);
      case OW_MESSAGEID_TYPE: return ow_marshal2_MessageId(buffer, bitbuffer, (ow_MessageId*)object);
      case OW_ACTIVEMQTEMPQUEUE_TYPE: return ow_marshal2_ActiveMQTempQueue(buffer, bitbuffer, (ow_ActiveMQTempQueue*)object);
      case OW_REMOVESUBSCRIPTIONINFO_TYPE: return ow_marshal2_RemoveSubscriptionInfo(buffer, bitbuffer, (ow_RemoveSubscriptionInfo*)object);
      case OW_SESSIONID_TYPE: return ow_marshal2_SessionId(buffer, bitbuffer, (ow_SessionId*)object);
      case OW_DATAARRAYRESPONSE_TYPE: return ow_marshal2_DataArrayResponse(buffer, bitbuffer, (ow_DataArrayResponse*)object);
      case OW_JOURNALQUEUEACK_TYPE: return ow_marshal2_JournalQueueAck(buffer, bitbuffer, (ow_JournalQueueAck*)object);
      case OW_RESPONSE_TYPE: return ow_marshal2_Response(buffer, bitbuffer, (ow_Response*)object);
      case OW_CONNECTIONERROR_TYPE: return ow_marshal2_ConnectionError(buffer, bitbuffer, (ow_ConnectionError*)object);
      case OW_CONSUMERINFO_TYPE: return ow_marshal2_ConsumerInfo(buffer, bitbuffer, (ow_ConsumerInfo*)object);
      case OW_XATRANSACTIONID_TYPE: return ow_marshal2_XATransactionId(buffer, bitbuffer, (ow_XATransactionId*)object);
      case OW_JOURNALTRACE_TYPE: return ow_marshal2_JournalTrace(buffer, bitbuffer, (ow_JournalTrace*)object);
      case OW_CONSUMERID_TYPE: return ow_marshal2_ConsumerId(buffer, bitbuffer, (ow_ConsumerId*)object);
      case OW_ACTIVEMQTEXTMESSAGE_TYPE: return ow_marshal2_ActiveMQTextMessage(buffer, bitbuffer, (ow_ActiveMQTextMessage*)object);
      case OW_SUBSCRIPTIONINFO_TYPE: return ow_marshal2_SubscriptionInfo(buffer, bitbuffer, (ow_SubscriptionInfo*)object);
      case OW_JOURNALTRANSACTION_TYPE: return ow_marshal2_JournalTransaction(buffer, bitbuffer, (ow_JournalTransaction*)object);
      case OW_CONTROLCOMMAND_TYPE: return ow_marshal2_ControlCommand(buffer, bitbuffer, (ow_ControlCommand*)object);
      case OW_LASTPARTIALCOMMAND_TYPE: return ow_marshal2_LastPartialCommand(buffer, bitbuffer, (ow_LastPartialCommand*)object);
      case OW_NETWORKBRIDGEFILTER_TYPE: return ow_marshal2_NetworkBridgeFilter(buffer, bitbuffer, (ow_NetworkBridgeFilter*)object);
      case OW_ACTIVEMQBYTESMESSAGE_TYPE: return ow_marshal2_ActiveMQBytesMessage(buffer, bitbuffer, (ow_ActiveMQBytesMessage*)object);
      case OW_WIREFORMATINFO_TYPE: return ow_marshal2_WireFormatInfo(buffer, bitbuffer, (ow_WireFormatInfo*)object);
      case OW_ACTIVEMQTEMPTOPIC_TYPE: return ow_marshal2_ActiveMQTempTopic(buffer, bitbuffer, (ow_ActiveMQTempTopic*)object);
      case OW_DISCOVERYEVENT_TYPE: return ow_marshal2_DiscoveryEvent(buffer, bitbuffer, (ow_DiscoveryEvent*)object);
      case OW_REPLAYCOMMAND_TYPE: return ow_marshal2_ReplayCommand(buffer, bitbuffer, (ow_ReplayCommand*)object);
      case OW_ACTIVEMQTOPIC_TYPE: return ow_marshal2_ActiveMQTopic(buffer, bitbuffer, (ow_ActiveMQTopic*)object);
      case OW_BROKERINFO_TYPE: return ow_marshal2_BrokerInfo(buffer, bitbuffer, (ow_BrokerInfo*)object);
      case OW_DESTINATIONINFO_TYPE: return ow_marshal2_DestinationInfo(buffer, bitbuffer, (ow_DestinationInfo*)object);
      case OW_SHUTDOWNINFO_TYPE: return ow_marshal2_ShutdownInfo(buffer, bitbuffer, (ow_ShutdownInfo*)object);
      case OW_DATARESPONSE_TYPE: return ow_marshal2_DataResponse(buffer, bitbuffer, (ow_DataResponse*)object);
      case OW_CONNECTIONCONTROL_TYPE: return ow_marshal2_ConnectionControl(buffer, bitbuffer, (ow_ConnectionControl*)object);
      case OW_KEEPALIVEINFO_TYPE: return ow_marshal2_KeepAliveInfo(buffer, bitbuffer, (ow_KeepAliveInfo*)object);
      case OW_FLUSHCOMMAND_TYPE: return ow_marshal2_FlushCommand(buffer, bitbuffer, (ow_FlushCommand*)object);
      case OW_CONSUMERCONTROL_TYPE: return ow_marshal2_ConsumerControl(buffer, bitbuffer, (ow_ConsumerControl*)object);
      case OW_JOURNALTOPICACK_TYPE: return ow_marshal2_JournalTopicAck(buffer, bitbuffer, (ow_JournalTopicAck*)object);
      case OW_BROKERID_TYPE: return ow_marshal2_BrokerId(buffer, bitbuffer, (ow_BrokerId*)object);
      case OW_MESSAGEDISPATCH_TYPE: return ow_marshal2_MessageDispatch(buffer, bitbuffer, (ow_MessageDispatch*)object);
      case OW_ACTIVEMQMAPMESSAGE_TYPE: return ow_marshal2_ActiveMQMapMessage(buffer, bitbuffer, (ow_ActiveMQMapMessage*)object);
      case OW_ACTIVEMQMESSAGE_TYPE: return ow_marshal2_ActiveMQMessage(buffer, bitbuffer, (ow_ActiveMQMessage*)object);
      case OW_REMOVEINFO_TYPE: return ow_marshal2_RemoveInfo(buffer, bitbuffer, (ow_RemoveInfo*)object);
      case OW_EXCEPTIONRESPONSE_TYPE: return ow_marshal2_ExceptionResponse(buffer, bitbuffer, (ow_ExceptionResponse*)object);
   }
   return APR_EGENERAL;
}

apr_status_t ow_unmarshal_object(ow_byte_array *buffer, ow_bit_buffer *bitbuffer, ow_DataStructure *object, apr_pool_t *pool)
{
   switch( object->structType ) {

      case OW_LOCALTRANSACTIONID_TYPE: return ow_unmarshal_LocalTransactionId(buffer, bitbuffer, (ow_LocalTransactionId*)object, pool);
      case OW_PARTIALCOMMAND_TYPE: return ow_unmarshal_PartialCommand(buffer, bitbuffer, (ow_PartialCommand*)object, pool);
      case OW_INTEGERRESPONSE_TYPE: return ow_unmarshal_IntegerResponse(buffer, bitbuffer, (ow_IntegerResponse*)object, pool);
      case OW_ACTIVEMQQUEUE_TYPE: return ow_unmarshal_ActiveMQQueue(buffer, bitbuffer, (ow_ActiveMQQueue*)object, pool);
      case OW_ACTIVEMQOBJECTMESSAGE_TYPE: return ow_unmarshal_ActiveMQObjectMessage(buffer, bitbuffer, (ow_ActiveMQObjectMessage*)object, pool);
      case OW_CONNECTIONID_TYPE: return ow_unmarshal_ConnectionId(buffer, bitbuffer, (ow_ConnectionId*)object, pool);
      case OW_CONNECTIONINFO_TYPE: return ow_unmarshal_ConnectionInfo(buffer, bitbuffer, (ow_ConnectionInfo*)object, pool);
      case OW_PRODUCERINFO_TYPE: return ow_unmarshal_ProducerInfo(buffer, bitbuffer, (ow_ProducerInfo*)object, pool);
      case OW_MESSAGEDISPATCHNOTIFICATION_TYPE: return ow_unmarshal_MessageDispatchNotification(buffer, bitbuffer, (ow_MessageDispatchNotification*)object, pool);
      case OW_SESSIONINFO_TYPE: return ow_unmarshal_SessionInfo(buffer, bitbuffer, (ow_SessionInfo*)object, pool);
      case OW_TRANSACTIONINFO_TYPE: return ow_unmarshal_TransactionInfo(buffer, bitbuffer, (ow_TransactionInfo*)object, pool);
      case OW_ACTIVEMQSTREAMMESSAGE_TYPE: return ow_unmarshal_ActiveMQStreamMessage(buffer, bitbuffer, (ow_ActiveMQStreamMessage*)object, pool);
      case OW_MESSAGEACK_TYPE: return ow_unmarshal_MessageAck(buffer, bitbuffer, (ow_MessageAck*)object, pool);
      case OW_PRODUCERID_TYPE: return ow_unmarshal_ProducerId(buffer, bitbuffer, (ow_ProducerId*)object, pool);
      case OW_MESSAGEID_TYPE: return ow_unmarshal_MessageId(buffer, bitbuffer, (ow_MessageId*)object, pool);
      case OW_ACTIVEMQTEMPQUEUE_TYPE: return ow_unmarshal_ActiveMQTempQueue(buffer, bitbuffer, (ow_ActiveMQTempQueue*)object, pool);
      case OW_REMOVESUBSCRIPTIONINFO_TYPE: return ow_unmarshal_RemoveSubscriptionInfo(buffer, bitbuffer, (ow_RemoveSubscriptionInfo*)object, pool);
      case OW_SESSIONID_TYPE: return ow_unmarshal_SessionId(buffer, bitbuffer, (ow_SessionId*)object, pool);
      case OW_DATAARRAYRESPONSE_TYPE: return ow_unmarshal_DataArrayResponse(buffer, bitbuffer, (ow_DataArrayResponse*)object, pool);
      case OW_JOURNALQUEUEACK_TYPE: return ow_unmarshal_JournalQueueAck(buffer, bitbuffer, (ow_JournalQueueAck*)object, pool);
      case OW_RESPONSE_TYPE: return ow_unmarshal_Response(buffer, bitbuffer, (ow_Response*)object, pool);
      case OW_CONNECTIONERROR_TYPE: return ow_unmarshal_ConnectionError(buffer, bitbuffer, (ow_ConnectionError*)object, pool);
      case OW_CONSUMERINFO_TYPE: return ow_unmarshal_ConsumerInfo(buffer, bitbuffer, (ow_ConsumerInfo*)object, pool);
      case OW_XATRANSACTIONID_TYPE: return ow_unmarshal_XATransactionId(buffer, bitbuffer, (ow_XATransactionId*)object, pool);
      case OW_JOURNALTRACE_TYPE: return ow_unmarshal_JournalTrace(buffer, bitbuffer, (ow_JournalTrace*)object, pool);
      case OW_CONSUMERID_TYPE: return ow_unmarshal_ConsumerId(buffer, bitbuffer, (ow_ConsumerId*)object, pool);
      case OW_ACTIVEMQTEXTMESSAGE_TYPE: return ow_unmarshal_ActiveMQTextMessage(buffer, bitbuffer, (ow_ActiveMQTextMessage*)object, pool);
      case OW_SUBSCRIPTIONINFO_TYPE: return ow_unmarshal_SubscriptionInfo(buffer, bitbuffer, (ow_SubscriptionInfo*)object, pool);
      case OW_JOURNALTRANSACTION_TYPE: return ow_unmarshal_JournalTransaction(buffer, bitbuffer, (ow_JournalTransaction*)object, pool);
      case OW_CONTROLCOMMAND_TYPE: return ow_unmarshal_ControlCommand(buffer, bitbuffer, (ow_ControlCommand*)object, pool);
      case OW_LASTPARTIALCOMMAND_TYPE: return ow_unmarshal_LastPartialCommand(buffer, bitbuffer, (ow_LastPartialCommand*)object, pool);
      case OW_NETWORKBRIDGEFILTER_TYPE: return ow_unmarshal_NetworkBridgeFilter(buffer, bitbuffer, (ow_NetworkBridgeFilter*)object, pool);
      case OW_ACTIVEMQBYTESMESSAGE_TYPE: return ow_unmarshal_ActiveMQBytesMessage(buffer, bitbuffer, (ow_ActiveMQBytesMessage*)object, pool);
      case OW_WIREFORMATINFO_TYPE: return ow_unmarshal_WireFormatInfo(buffer, bitbuffer, (ow_WireFormatInfo*)object, pool);
      case OW_ACTIVEMQTEMPTOPIC_TYPE: return ow_unmarshal_ActiveMQTempTopic(buffer, bitbuffer, (ow_ActiveMQTempTopic*)object, pool);
      case OW_DISCOVERYEVENT_TYPE: return ow_unmarshal_DiscoveryEvent(buffer, bitbuffer, (ow_DiscoveryEvent*)object, pool);
      case OW_REPLAYCOMMAND_TYPE: return ow_unmarshal_ReplayCommand(buffer, bitbuffer, (ow_ReplayCommand*)object, pool);
      case OW_ACTIVEMQTOPIC_TYPE: return ow_unmarshal_ActiveMQTopic(buffer, bitbuffer, (ow_ActiveMQTopic*)object, pool);
      case OW_BROKERINFO_TYPE: return ow_unmarshal_BrokerInfo(buffer, bitbuffer, (ow_BrokerInfo*)object, pool);
      case OW_DESTINATIONINFO_TYPE: return ow_unmarshal_DestinationInfo(buffer, bitbuffer, (ow_DestinationInfo*)object, pool);
      case OW_SHUTDOWNINFO_TYPE: return ow_unmarshal_ShutdownInfo(buffer, bitbuffer, (ow_ShutdownInfo*)object, pool);
      case OW_DATARESPONSE_TYPE: return ow_unmarshal_DataResponse(buffer, bitbuffer, (ow_DataResponse*)object, pool);
      case OW_CONNECTIONCONTROL_TYPE: return ow_unmarshal_ConnectionControl(buffer, bitbuffer, (ow_ConnectionControl*)object, pool);
      case OW_KEEPALIVEINFO_TYPE: return ow_unmarshal_KeepAliveInfo(buffer, bitbuffer, (ow_KeepAliveInfo*)object, pool);
      case OW_FLUSHCOMMAND_TYPE: return ow_unmarshal_FlushCommand(buffer, bitbuffer, (ow_FlushCommand*)object, pool);
      case OW_CONSUMERCONTROL_TYPE: return ow_unmarshal_ConsumerControl(buffer, bitbuffer, (ow_ConsumerControl*)object, pool);
      case OW_JOURNALTOPICACK_TYPE: return ow_unmarshal_JournalTopicAck(buffer, bitbuffer, (ow_JournalTopicAck*)object, pool);
      case OW_BROKERID_TYPE: return ow_unmarshal_BrokerId(buffer, bitbuffer, (ow_BrokerId*)object, pool);
      case OW_MESSAGEDISPATCH_TYPE: return ow_unmarshal_MessageDispatch(buffer, bitbuffer, (ow_MessageDispatch*)object, pool);
      case OW_ACTIVEMQMAPMESSAGE_TYPE: return ow_unmarshal_ActiveMQMapMessage(buffer, bitbuffer, (ow_ActiveMQMapMessage*)object, pool);
      case OW_ACTIVEMQMESSAGE_TYPE: return ow_unmarshal_ActiveMQMessage(buffer, bitbuffer, (ow_ActiveMQMessage*)object, pool);
      case OW_REMOVEINFO_TYPE: return ow_unmarshal_RemoveInfo(buffer, bitbuffer, (ow_RemoveInfo*)object, pool);
      case OW_EXCEPTIONRESPONSE_TYPE: return ow_unmarshal_ExceptionResponse(buffer, bitbuffer, (ow_ExceptionResponse*)object, pool);
   }
   return APR_EGENERAL;
}
