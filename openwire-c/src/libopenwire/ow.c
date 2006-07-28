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


#include <stdlib.h>

#include "ow.h"
#include "ow_command_types_v1.h"

/************************************************************************
 * Transport Connect and Disconnect
 ************************************************************************/
apr_status_t ow_connect(ow_transport **transport_ref, const char *hostname, int port, apr_pool_t *pool)
{
	apr_status_t rc;
	int socket_family;
	ow_transport *transport=NULL;
   
	//
	// Allocate the transport and a memory pool for the transport.
	//
	transport = apr_pcalloc(pool, sizeof(transport));
	if( transport == NULL )
		return APR_ENOMEM;
   
#define CHECK_SUCCESS if( rc!=APR_SUCCESS ) { return rc; }
   
	// Look up the remote address
	rc = apr_sockaddr_info_get(&transport->remote_sa, hostname, APR_UNSPEC, port, 0, pool);
	CHECK_SUCCESS;
	
	// Create and Connect the socket.
	socket_family = transport->remote_sa->sa.sin.sin_family;
	rc = apr_socket_create(&transport->socket, socket_family, SOCK_STREAM, APR_PROTO_TCP, pool);
	CHECK_SUCCESS;	
   rc = apr_socket_connect(transport->socket, transport->remote_sa);
	CHECK_SUCCESS;
   
   // Get the Socket Info
   rc = apr_socket_addr_get(&transport->remote_sa, APR_REMOTE, transport->socket);
	CHECK_SUCCESS;
   rc = apr_sockaddr_ip_get(&transport->remote_ip, transport->remote_sa);
	CHECK_SUCCESS;
   rc = apr_socket_addr_get(&transport->local_sa, APR_LOCAL, transport->socket);
	CHECK_SUCCESS;
   rc = apr_sockaddr_ip_get(&transport->local_ip, transport->local_sa);
	CHECK_SUCCESS;	
   
   // Set socket options.
   //	rc = apr_socket_timeout_set( transport->socket, 2*APR_USEC_PER_SEC);
   //	CHECK_SUCCESS;
   
#undef CHECK_SUCCESS
   
	*transport_ref = transport;
	return rc;	
}

apr_status_t ow_disconnect(ow_transport **transport_ref)
{
   apr_status_t result, rc;
	ow_transport *transport = *transport_ref;
   
   if( transport_ref == NULL || *transport_ref==NULL )
      return APR_EGENERAL;
   
	result = APR_SUCCESS;	
   rc = apr_socket_shutdown(transport->socket, APR_SHUTDOWN_WRITE);	
	if( result!=APR_SUCCESS )
		result = rc;
   
   if( transport->socket != NULL ) {
      rc = apr_socket_close(transport->socket);
      if( result!=APR_SUCCESS )
         result = rc;
      transport->socket=NULL;
   }   
	*transport_ref=NULL;
	return rc;	
}

/************************************************************************
 * Read and Write byte data from a transport
 ************************************************************************/
apr_status_t ow_write(ow_transport *transport, const ow_byte *data, apr_size_t size)
{
   apr_size_t remaining = size;
   size=0;
	while( remaining>0 ) {
		apr_size_t length = remaining;
		apr_status_t rc = apr_socket_send(transport->socket, data, &length);
      data+=length;
      remaining -= length;
      //      size += length;
      if( rc != APR_SUCCESS ) {
         return rc;
      }
	}
	return APR_SUCCESS;
}

apr_status_t ow_read(ow_transport *transport, ow_byte *data, apr_size_t size)
{
   apr_size_t remaining = size;
   size=0;
	while( remaining>0 ) {
		apr_size_t length = remaining;
      apr_status_t rc = apr_socket_recv(transport->socket, data, &length);
      data+=length;
      remaining -= length;
      //      size += length;
      if( rc != APR_SUCCESS ) {
         return rc;
      }
	}
	return APR_SUCCESS;
}

/************************************************************************
 * Read and Write data structures from a transport
 ************************************************************************/
apr_status_t ow_write_command(ow_transport *transport, ow_DataStructure *object)
{
   apr_pool_t *pool;
   apr_status_t rc;
   ow_byte_buffer *buffer;
   ow_bit_buffer *bitbuffer;
   ow_int command_size;

   rc = apr_pool_create(&pool, NULL);
   if( rc != APR_SUCCESS ) 
      return rc;
   
   buffer = ow_byte_buffer_create(pool);
   bitbuffer = ow_bit_buffer_create(pool);
   
#define CHECK_SUCCESS if( rc!=APR_SUCCESS ) { apr_pool_destroy(pool); return rc; }
   
   rc = ow_byte_buffer_append_byte(buffer, object->structType);
   CHECK_SUCCESS;
   rc = ow_marshal1_object(bitbuffer, object);
   CHECK_SUCCESS;
   rc = ow_byte_buffer_append_bit_buffer(buffer, bitbuffer);
   CHECK_SUCCESS;
   rc = ow_marshal2_object(buffer, bitbuffer, object);
   CHECK_SUCCESS;
   
   command_size = ENDIAN_FLIP_INT(buffer->size);

   rc = ow_write(transport, (ow_byte*)&command_size, sizeof(command_size));   
   CHECK_SUCCESS;
   rc = ow_byte_buffer_write(buffer, transport);   
   CHECK_SUCCESS;
#undef CHECK_SUCCESS      
   
	apr_pool_destroy(pool);
   return rc;
}

apr_status_t ow_read_command(ow_transport *transport, ow_DataStructure **object, apr_pool_t *pool)
{
   apr_status_t rc;
   ow_byte_array data;
   ow_bit_buffer *bitbuffer;
   ow_byte type;
   
#define CHECK_SUCCESS if( rc!=APR_SUCCESS ) { return rc; }
   // Find out how much data to read in.
   rc = ow_read(transport, (ow_byte*)&data.size, sizeof(data.size));
   CHECK_SUCCESS;

   data.size = ENDIAN_FLIP_INT(data.size);
   
   // Read in the data
   data.values = apr_pcalloc(pool,data.size);   
   rc = ow_read(transport, data.values, data.size);
   CHECK_SUCCESS;
   
   rc = ow_byte_array_read_byte(&data, &type);
   CHECK_SUCCESS;
   rc = ow_byte_array_read_bit_buffer(&data, &bitbuffer, pool);
   CHECK_SUCCESS;      
#undef CHECK_SUCCESS
   
   *object = ow_create_object(type, pool);
   if( *object == 0 )
      return APR_ENOMEM;
   
   rc = ow_unmarshal_object(&data, bitbuffer, *object, pool);
   if( rc != APR_SUCCESS )
      *object = 0;

   return rc;
}


/************************************************************************
 * Core data structure functions.
 ************************************************************************/
ow_byte_array *ow_byte_array_create(apr_pool_t *pool, apr_size_t size) 
{
   ow_byte_array *b = apr_pcalloc(pool,sizeof(ow_byte_array));
   if( b!=NULL ) {
      b->size=size;
      b->values = apr_pcalloc(pool,size*sizeof(ow_byte));
      if( b->values == NULL )
         return NULL;
   }
   return b;
}

ow_byte_array *ow_byte_array_create_with_data(apr_pool_t *pool, apr_size_t size, ow_byte *data) 
{
   ow_byte_array *b = apr_pcalloc(pool,sizeof(ow_byte_array));
   if( b!=NULL ) {
      b->size=size;
      b->values = data;
   }
   return b;
}

ow_string *ow_string_create(apr_pool_t *pool, apr_size_t size) 
{
   ow_string *b = apr_pcalloc(pool,sizeof(ow_string));
   if( b!=NULL ) {
      b->size=size;
      b->values = apr_pcalloc(pool,size*sizeof(ow_char));
      if( b->values == NULL )
         return NULL;
   }
   return b;
}

ow_string *ow_string_create_from_cstring(apr_pool_t *pool, const ow_char *source) 
{
   ow_string *b = ow_string_create(pool,strlen(source));
   if( b!=NULL ) {
      strcpy(b->values, source);
   }
   return b;
}

ow_DataStructure_array *ow_DataStructure_array_create(apr_pool_t *pool, apr_size_t size) 
{
   ow_DataStructure_array *b = apr_pcalloc(pool,sizeof(ow_DataStructure_array));
   if( b!=NULL ) {
      b->size=size;
      b->values = apr_pcalloc(pool,size*sizeof(ow_DataStructure*));
      if( b->values == NULL )
         return NULL;
   }
   return b;
}

ow_throwable *ow_throwable_create(apr_pool_t *pool) 
{
   ow_throwable *b = apr_pcalloc(pool,sizeof(ow_throwable));
   return b;
}

