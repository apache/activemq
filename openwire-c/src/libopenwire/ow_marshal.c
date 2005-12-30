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

#include "ow.h"
#include "ow_commands_v1.h"

/************************************************************************
 * marshal/unmarshal nested objects
 ************************************************************************/

ow_boolean ow_is_a_MarshallAware(ow_DataStructure *object) {
   return ow_is_a_Message(object);
}

apr_status_t ow_marshal1_nested_object(ow_bit_buffer *bitbuffer, ow_DataStructure *object)
{
   ow_bit_buffer_append(bitbuffer, object!=0);
   if( object == 0 )
      return APR_SUCCESS;
   
   if( ow_is_a_MarshallAware(object) ) {
      ow_bit_buffer_append(bitbuffer, 0);
   }
      
   return ow_marshal1_object(bitbuffer, object);
}
apr_status_t ow_marshal2_nested_object(ow_byte_buffer *buffer, ow_bit_buffer *bitbuffer, ow_DataStructure *object)
{
   apr_status_t rc;
   if( !ow_bit_buffer_read(bitbuffer) ) {
      return APR_SUCCESS;
   } 

   rc = ow_byte_buffer_append_byte(buffer, object->structType);
   if( rc != APR_SUCCESS )
      return rc;
   
   if( ow_is_a_MarshallAware(object) ) {
      ow_bit_buffer_read(bitbuffer);
   }
   
   rc = ow_marshal2_object(buffer, bitbuffer, object);
   return rc;
}

apr_status_t ow_unmarshal_nested_object(ow_byte_array *data, ow_bit_buffer *bitbuffer, ow_DataStructure **object, apr_pool_t *pool)
{
   
   if( ow_bit_buffer_read(bitbuffer) ) {
      apr_status_t rc;
      ow_byte type;
      
      
      rc = ow_byte_array_read_byte(data, &type);
      if( rc!=APR_SUCCESS ) { return rc; }
      
      *object = ow_create_object(type, pool);
      if( object == 0 )
         return APR_ENOMEM;
      
      if( ow_is_a_MarshallAware(*object) && ow_bit_buffer_read(bitbuffer) ) {
         ow_bit_buffer *bitbuffer2;
         ow_int size;
         
         rc = ow_byte_array_read_int(data, &size);
         if( rc!=APR_SUCCESS ) { *object=0; return rc; }
         rc = ow_byte_array_read_byte(data, &type);
         if( rc!=APR_SUCCESS ) { *object=0; return rc; }
         
         rc = ow_byte_array_read_bit_buffer(data, &bitbuffer2, pool);
         
         rc = ow_unmarshal_object(data, bitbuffer2, *object, pool);
         if( rc != APR_SUCCESS )
            *object = 0;
         
      } else {
         
         rc = ow_unmarshal_object(data, bitbuffer, *object, pool);
         if( rc != APR_SUCCESS )
            *object = 0;
         
      }
      
      return rc;
      
   } else {
      *object=0;
      return APR_SUCCESS;
   }
      
   
}

/************************************************************************
 * marshal/unmarshal cached objects
 ************************************************************************/
apr_status_t ow_marshal1_cached_object(ow_bit_buffer *bitbuffer, ow_DataStructure *object) {
   return ow_marshal1_nested_object(bitbuffer, object);
}
apr_status_t ow_marshal2_cached_object(ow_byte_buffer *buffer, ow_bit_buffer *bitbuffer, ow_DataStructure *object)
{
   return ow_marshal2_nested_object(buffer, bitbuffer, object);
}
apr_status_t ow_unmarshal_cached_object(ow_byte_array *data, ow_bit_buffer *bitbuffer, ow_DataStructure **object, apr_pool_t *pool) {
   return ow_unmarshal_nested_object(data, bitbuffer, object, pool);
}


/************************************************************************
 * marshal/unmarshal strings
 ************************************************************************/
apr_status_t ow_marshal1_string(ow_bit_buffer *buffer, ow_string *value) {
   ow_bit_buffer_append(buffer, value!=NULL);
   if( value != NULL ) {
      // this would be true if we were sure it was an ascii string (optimization to avoid utf-8 encode)
      ow_bit_buffer_append(buffer, 0);
   }
   return APR_SUCCESS;
}
apr_status_t ow_marshal2_string(ow_byte_buffer *buffer, ow_bit_buffer *bitbuffer, ow_string *value) {
   apr_status_t rc = APR_SUCCESS;
   if( ow_bit_buffer_read(bitbuffer) ) {
      ow_bit_buffer_read(bitbuffer);
      rc = ow_byte_buffer_append_short(buffer, value->size);
      if( rc != APR_SUCCESS ) return rc;
      rc = ow_byte_buffer_append(buffer, value->values, value->size, 0);
   }
   return rc;
}
apr_status_t ow_unmarshal_string(ow_byte_array *buffer, ow_bit_buffer *bitbuffer, ow_string **value, apr_pool_t *pool) {
   apr_status_t rc;
   ow_short size;
   if( ow_bit_buffer_read(bitbuffer) ) {
      
      // tells us if it is an ascii string (optimization to avoid utf-8 decode)
      ow_bit_buffer_read(bitbuffer);
      
      rc = ow_byte_array_read_short(buffer, &size);
      if( rc != APR_SUCCESS ) 
         return rc;
      
      if( buffer->size < size )
         return APR_EOF;
      
      *value = apr_pcalloc(pool, sizeof(ow_string));
      if( *value == NULL )
         return APR_ENOMEM;
      
      (*value)->size = size;
      (*value)->values = buffer->values;
      
      buffer->values += size;
      buffer->size -= size;
      
   } else {
      *value=NULL;
   }
   return APR_SUCCESS;
}

/************************************************************************
* marshal/unmarshal variable size ow_byte_array
************************************************************************/
apr_status_t ow_marshal2_byte_array(ow_byte_buffer *buffer, ow_bit_buffer *bitbuffer, ow_byte_array *value) {
   apr_status_t rc = APR_SUCCESS;
   if( ow_bit_buffer_read(bitbuffer) ) {
      rc = ow_byte_buffer_append_int(buffer, value->size);
      if( rc != APR_SUCCESS )
         return rc;
      rc = ow_byte_buffer_append(buffer, value->values, value->size, 0);
   }
   return rc;
}
apr_status_t ow_unmarshal_byte_array(ow_byte_array *buffer,  ow_bit_buffer *bitbuffer, ow_byte_array **value, apr_pool_t *pool) {
   
   if( ow_bit_buffer_read(bitbuffer) ) {
      
      apr_status_t rc;
      ow_int size;
      
      rc = ow_byte_array_read_int(buffer, &size);
      if( rc != APR_SUCCESS ) 
         return rc;
      
      if( buffer->size < size )
         return APR_EOF;
      
      *value = apr_pcalloc(pool, sizeof(ow_byte_array));
      if( *value == NULL )
         return APR_ENOMEM;
      
      (*value)->size = size;
      (*value)->values = buffer->values;
      
      buffer->values += size;
      buffer->size -= size;
      
   } else {
      *value=NULL;
   }
   return APR_SUCCESS;
}

/************************************************************************
 * marshal/unmarshal constant size ow_byte_array
 ************************************************************************/
apr_status_t ow_marshal2_byte_array_const_size(ow_byte_buffer *buffer, ow_byte_array *value, ow_int size) {
   apr_status_t rc;
   if( value->size != size ) {
      return APR_EGENERAL;
   }
   rc = ow_byte_buffer_append(buffer, value->values, size, 0);
   return rc;
}
apr_status_t ow_unmarshal_byte_array_const_size(ow_byte_array *buffer, ow_byte_array **value, ow_int size, apr_pool_t *pool) {
   if( buffer->size < size )
      return APR_EOF;
   
   *value = apr_pcalloc(pool, sizeof(ow_byte_array));
   if( *value == NULL )
      return APR_ENOMEM;
   
   (*value)->size = size;
   (*value)->values = buffer->values;
   
   buffer->values += size;
   buffer->size -= size;
   return APR_SUCCESS;
}

/************************************************************************
 * marshal/unmarshal a DataStructure stucture
 ************************************************************************/
apr_status_t ow_marshal1_DataStructure(ow_bit_buffer *buffer, ow_DataStructure *object) {
   return APR_SUCCESS;
}
apr_status_t ow_marshal2_DataStructure(ow_byte_buffer *buffer, ow_bit_buffer *bitbuffer, ow_DataStructure *object) {
   return APR_SUCCESS;
}
apr_status_t ow_unmarshal_DataStructure(ow_byte_array *buffer, ow_bit_buffer *bitbuffer, ow_DataStructure *object,  apr_pool_t *pool)
{
	return APR_SUCCESS;
}

/************************************************************************
* marshal/unmarshal a variable size DataStructure_array
************************************************************************/
apr_status_t ow_marshal1_DataStructure_array(ow_bit_buffer *buffer, ow_DataStructure_array *value) {
   apr_status_t rc = APR_SUCCESS;
   if( value != NULL ) {
      ow_bit_buffer_append(buffer, 1);
      rc = ow_marshal1_DataStructure_array_const_size(buffer, value, value->size);
   } else {
      ow_bit_buffer_append(buffer, 0);
   }
   return rc;
}
apr_status_t ow_marshal2_DataStructure_array(ow_byte_buffer *buffer, ow_bit_buffer *bitbuffer, ow_DataStructure_array *value) {
   apr_status_t rc = APR_SUCCESS;
   if( ow_bit_buffer_read(bitbuffer) ) {
      rc = ow_byte_buffer_append_short(buffer, value->size);
      if( rc != APR_SUCCESS )
         return rc;
      rc = ow_marshal2_DataStructure_array_const_size(buffer, bitbuffer, value, value->size);
   }
   return rc;
}
apr_status_t ow_unmarshal_DataStructure_array(ow_byte_array *buffer, ow_bit_buffer *bitbuffer, ow_DataStructure_array **value, apr_pool_t *pool) {
   
   apr_status_t rc;
   ow_short size;
   
   if( ow_bit_buffer_read(bitbuffer) ) {
      rc = ow_byte_array_read_short(buffer, &size);
      if( rc != APR_SUCCESS ) 
         return rc;
      return ow_unmarshal_DataStructure_array_const_size(buffer, bitbuffer, value, size, pool);
   } else {
      *value=NULL;
   }
   return APR_SUCCESS;
}

/************************************************************************
 * marshal/unmarshal a constant size DataStructure_array
 ************************************************************************/
apr_status_t ow_marshal1_DataStructure_array_const_size(ow_bit_buffer *buffer, ow_DataStructure_array *value, ow_int size) {
   apr_status_t rc;
   ow_int i;
   for( i=0; i < value->size; i++ ) {
      rc = ow_marshal1_nested_object(buffer, value->values[i]);
      if( rc != APR_SUCCESS )
         return rc;
   }
   return APR_SUCCESS;
}
apr_status_t ow_marshal2_DataStructure_array_const_size(ow_byte_buffer *buffer, ow_bit_buffer *bitbuffer, ow_DataStructure_array *value, ow_int size) {
   apr_status_t rc;
   ow_int i;
   if( value->size != size ) {
      return APR_EGENERAL;
   }
   for( i=0; i < value->size; i++ ) {
      rc = ow_marshal2_nested_object(buffer, bitbuffer, value->values[i]);
      if( rc != APR_SUCCESS )
         return rc;
   }
   return APR_SUCCESS;
}
apr_status_t ow_unmarshal_DataStructure_array_const_size(ow_byte_array *buffer, ow_bit_buffer *bitbuffer, ow_DataStructure_array **value, ow_int size, apr_pool_t *pool) {
   
   ow_int i;   
   *value = apr_pcalloc(pool, sizeof(ow_DataStructure_array));
   if( *value == NULL )
      return APR_ENOMEM;
   
   (*value)->size = size;   
   (*value)->values = apr_pcalloc(pool, sizeof(ow_DataStructure*)*size);
   
   for( i=0; i < (*value)->size; i++ ) {
      apr_status_t rc = ow_unmarshal_nested_object(buffer, bitbuffer, &((*value)->values[i]), pool);
      if( rc != APR_SUCCESS )
         return rc;
   }
   
   return APR_SUCCESS;
}

/************************************************************************
 * marshal/unmarshal throwable
 ************************************************************************/
apr_status_t ow_marshal1_throwable(ow_bit_buffer *buffer, ow_throwable *value) {
   ow_bit_buffer_append(buffer, value!=NULL);
   return APR_SUCCESS;
}
apr_status_t ow_marshal2_throwable(ow_byte_buffer *buffer, ow_bit_buffer *bitbuffer, ow_throwable *value) {
   apr_status_t rc = APR_SUCCESS;
   if( ow_bit_buffer_read(bitbuffer) ) {
      //TODO: needs to be implemented.
   }
   return rc;
}
apr_status_t ow_unmarshal_throwable(ow_byte_array *buffer, ow_bit_buffer *bitbuffer, ow_throwable **value, apr_pool_t *pool) {
   if( ow_bit_buffer_read(bitbuffer) ) {      
      //TODO: needs to be implemented.      
   } else {
      *value=NULL;
   }
   return APR_SUCCESS;
}

/************************************************************************
* marshal/unmarshal long
************************************************************************/
apr_status_t ow_marshal1_long(ow_bit_buffer *buffer, ow_long value) {
   if( value == 0 ) {
      ow_bit_buffer_append(buffer, 0);
      ow_bit_buffer_append(buffer, 0);
   } else if ( (value & 0xFFFFFFFFFFFF0000ll ) == 0 ) {
      ow_bit_buffer_append(buffer, 0);
      ow_bit_buffer_append(buffer, 1);
   } else if ( (value & 0xFFFFFFFF00000000ll ) == 0) {
      ow_bit_buffer_append(buffer, 1);
      ow_bit_buffer_append(buffer, 0);
   } else {
      ow_bit_buffer_append(buffer, 1);
      ow_bit_buffer_append(buffer, 1);
   }
   return APR_SUCCESS;
}

apr_status_t ow_marshal2_long(ow_byte_buffer *buffer, ow_bit_buffer *bitbuffer, ow_long value) {
   apr_status_t rc = APR_SUCCESS;
   if(  ow_bit_buffer_read(bitbuffer) ) {
      if(  ow_bit_buffer_read(bitbuffer) ) {
         ow_byte_buffer_append_long(buffer,value);
      } else {
         ow_byte_buffer_append_int(buffer,value);
      }
   } else {
      if( ow_bit_buffer_read(bitbuffer) ) {
         ow_byte_buffer_append_short(buffer,value);
      }
   }
   return rc;
}
apr_status_t ow_unmarshal_long(ow_byte_array *buffer, ow_bit_buffer *bitbuffer, ow_long *value, apr_pool_t *pool) {
   if( ow_bit_buffer_read(bitbuffer) ) {
      if( ow_bit_buffer_read(bitbuffer) ) {
         ow_long t;
         ow_byte_array_read_long(buffer, &t);
         *value = t;
      } else {
         ow_int t;
         ow_byte_array_read_int(buffer, &t);
         *value = t;
      }
   } else {
      if( ow_bit_buffer_read(bitbuffer) ) {
         ow_short t;
         ow_byte_array_read_short(buffer, &t);
         *value = t;
      } else {
         *value=0;
      }
   }
   return APR_SUCCESS;
}


