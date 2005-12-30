/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
#include "amqcs.h"

#include <apr.h>
#include "amqcs.h"

#ifdef WIN32
#define snprintf _snprintf
#endif

static ow_byte ACTIVEMQ_MAGIC [] = {0x41,0x63,0x74,0x69,0x76,0x65,0x4D,0x51}; // ActiveMQ

/**
* An ActiveMQ Single Threaded Client Connection
 */ 
struct amqcs_connection {
   ow_transport *transport;
   ow_ConnectionInfo *info;
   ow_short lastCommandId;
   ow_boolean useAsyncSend;
   ow_boolean useMessageTimestamps;
   ow_ProducerId *producerId;
   ow_long lastMessageId;
   ow_TransactionId *transactionId;
};

apr_status_t amqcs_async_send(amqcs_connection *connection, ow_BaseCommand *object) {   
   object->commandId = ++(connection->lastCommandId);
   object->responseRequired = 0;
   return ow_write_command(connection->transport, (ow_DataStructure*)object);;
}

apr_status_t amqcs_sync_send(amqcs_connection *connection, ow_BaseCommand *object, apr_pool_t *pool) {
   
   apr_status_t rc;
   ow_DataStructure *response;
   
   object->commandId = ++(connection->lastCommandId);
   object->responseRequired = 1;
   rc = ow_write_command(connection->transport, (ow_DataStructure*)object);
   if( rc != APR_SUCCESS )
      return rc;
   
   for(;;) {
      rc = ow_read_command(connection->transport,&response,pool);
      if( rc != APR_SUCCESS )
         return rc;
      
      if( ow_is_a_Message(response) ) {
         //TODO: implement message dispatching.
         printf("Got a message.\n");
      }
      
      if( ow_is_a_Response(response) ) {
         ow_Response *r = (ow_Response *)response;
         if( r->correlationId == object->commandId ) {
            if( ow_is_a_ExceptionResponse(response) ) {
               return APR_EGENERAL;
            }
            return APR_SUCCESS;
         }
      }
   }
}

ow_ConnectionId *create_ConnectionId(amqcs_connection *connection, apr_pool_t *pool) {
   
   ow_ConnectionId *rc;
   char buff[255];
   apr_time_t now = apr_time_now();
   
   snprintf(buff, sizeof(buff), 
            "ID:%s:%d:%s:%d:%lld", 
            connection->transport->local_ip, 
            connection->transport->local_sa->port,
            connection->transport->remote_ip, 
            connection->transport->remote_sa->port,
            now
            );
   
   rc = ow_ConnectionId_create(pool);   
   rc->connectionId = ow_string_create_from_cstring(pool, buff);   
   return rc;
}

ow_ProducerId *create_ProducerId(amqcs_connection *connection, apr_pool_t *pool) {   
   ow_ProducerId *rc;
   rc = ow_ProducerId_create(pool);   
   rc->connectionId = connection->info->connectionId->connectionId;
   rc->sessionId = -1;
   rc->producerId = 1;
   return rc;
}

ow_WireFormatInfo *create_WireFormatInfo(apr_pool_t *pool) {
   ow_WireFormatInfo *info = ow_WireFormatInfo_create(pool);
   info->version = OW_WIREFORMAT_VERSION;
   info->options = 0;
   info->magic = ow_byte_array_create_with_data(pool, 8, ACTIVEMQ_MAGIC);
   return info;
}

apr_status_t amqcs_connect(amqcs_connection **conn, amqcs_connect_options *options, apr_pool_t *pool) {
   
   apr_status_t rc;
   ow_DataStructure *command;
   amqcs_connection *connection;
   
   apr_pool_t *temp_pool;
   
   rc = apr_pool_create(&temp_pool, pool);
   if( rc != APR_SUCCESS ) 
      return rc;
   
#define FAIL(rc) { apr_pool_destroy(temp_pool); return rc; }
#define CHECK_SUCCESS if( rc!=APR_SUCCESS ) { FAIL(rc); }
   
   connection = apr_pcalloc(pool, sizeof(amqcs_connection));   
   if( connection == NULL )
      FAIL(APR_ENOMEM);
   
	rc = ow_connect(&connection->transport, options->hostname, options->port, pool);
   CHECK_SUCCESS;
   
   // Negociate the WireFormat 
   rc = ow_write_command(connection->transport, (ow_DataStructure*)create_WireFormatInfo(temp_pool));
   CHECK_SUCCESS;
   rc = ow_read_command(connection->transport, &command, temp_pool);
   CHECK_SUCCESS;
   
   // Expecting a WireFormat back from the broker.
   if( command->structType != OW_WIREFORMATINFO_TYPE ) 
      FAIL(APR_EGENERAL);
   
   {
      // Verify that it's a valid wireformat
      ow_WireFormatInfo *info = (ow_WireFormatInfo*)command;
      if( memcmp(ACTIVEMQ_MAGIC, info->magic->values, sizeof(ACTIVEMQ_MAGIC)) !=0 ) {
         FAIL(APR_EGENERAL);
      }
      // We can not connect to brokers that lower wireformat version
      if( info->version < OW_WIREFORMAT_VERSION ) {
         FAIL(APR_EGENERAL);
      }
   }
   
   // Send the Connection Info
   {
      connection->info = ow_ConnectionInfo_create(pool);
      connection->info->connectionId = create_ConnectionId(connection, pool);
      if( strlen(options->clientId)>0 ) {
         connection->info->clientId = ow_string_create_from_cstring(pool, options->clientId);
      }
      if( strlen(options->userId)>0 ) {
         connection->info->userName = ow_string_create_from_cstring(pool, options->userId);
      }
      if( strlen(options->password)>0 ) {
         connection->info->password = ow_string_create_from_cstring(pool, options->password);      
      }
      rc = amqcs_sync_send( connection, (ow_BaseCommand*)connection->info, temp_pool );
      CHECK_SUCCESS;
   }
   
   connection->useAsyncSend = options->useAsyncSend;
   connection->producerId = create_ProducerId(connection, pool);
   
#undef CHECK_SUCCESS
#undef FAIL
      
   *conn = connection;
   return APR_SUCCESS;
}

apr_status_t amqcs_disconnect(amqcs_connection **connection) {
   apr_status_t rc;
   apr_pool_t *temp_pool;   

   if( connection == NULL || *connection==NULL )
      return APR_EGENERAL;
   
   rc = apr_pool_create(&temp_pool, NULL);
   if( rc == APR_SUCCESS ) {
      // Send the RemoveInfo packet for the connection.
      {
         ow_RemoveInfo *info = ow_RemoveInfo_create(temp_pool);
         info->objectId = (ow_DataStructure*)(*connection)->info->connectionId;
         amqcs_async_send(*connection, (ow_BaseCommand*)info);
      }      
      // Send the Shutdown packet.
      {
         ow_ShutdownInfo *info = ow_ShutdownInfo_create(temp_pool);
         amqcs_async_send(*connection, (ow_BaseCommand*)info);
      }
   }   
   
   rc = ow_disconnect(&(*connection)->transport);
   *connection = NULL;
   return rc;
}

apr_status_t amqcs_send(amqcs_connection *connection, ow_ActiveMQDestination *dest,  ow_ActiveMQMessage *message, 
                        ow_int deliveryMode, ow_int priority, ow_long timeToLive, apr_pool_t *pool) {
   
   apr_pool_t *temp_pool;   
   apr_status_t rc;

   rc = apr_pool_create(&temp_pool, NULL);
   if( rc!=APR_SUCCESS )
      return rc;
   
   message->messageId = ow_MessageId_create(pool);
   message->messageId->producerId = connection->producerId;
   message->messageId->producerSequenceId = ++(connection->lastMessageId);
   message->destination = dest;
   message->priority = priority;
   message->persistent = deliveryMode==2;
   if( connection->useMessageTimestamps ) {
      message->timestamp = apr_time_now();
      if( timeToLive > 0 ) {
         message->expiration = message->timestamp+timeToLive;
      }
   }
   
   message->producerId = connection->producerId;
   message->originalDestination = dest;
   message->destination = dest;
   message->originalTransactionId = connection->transactionId;
   message->transactionId = connection->transactionId;

#define CHECK_SUCCESS if( rc!=APR_SUCCESS ) {apr_pool_destroy(temp_pool); return rc;}
      
   if( connection->useAsyncSend ) {
      rc = amqcs_async_send( connection, (ow_BaseCommand*)message);
      CHECK_SUCCESS;
   } else {
      rc = amqcs_sync_send( connection, (ow_BaseCommand*)message, temp_pool );
      CHECK_SUCCESS;
   }
   
#undef CHECK_SUCCESS

   apr_pool_destroy(temp_pool);
   return APR_SUCCESS;
}

