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

/************************************************************************
 * bit_buffer functions
 ************************************************************************/

ow_bit_buffer *ow_bit_buffer_create(apr_pool_t *pool) 
{
   ow_bit_buffer *b = apr_pcalloc(pool,sizeof(ow_bit_buffer));
   return b;
}

ow_boolean ow_bit_buffer_read(ow_bit_buffer *buffer) {
   ow_byte b = buffer->values[buffer->arrayPos];
   ow_boolean rc = ((b>>buffer->bytePos)&0x01)!=0;
   buffer->bytePos++;
   if( buffer->bytePos >= 8 ) {
      buffer->bytePos=0;
      buffer->arrayPos++;
   }
   return rc;
}

void ow_bit_buffer_append(ow_bit_buffer *buffer, ow_boolean value) {
   if( value ) {
      buffer->values[buffer->arrayPos] |= (0x01 << buffer->bytePos); 
   }
   if( buffer->bytePos == 0 ) {
      buffer->arrayLimit++;
   }
   buffer->bytePos++;
   if( buffer->bytePos >= 8 ) {
      buffer->bytePos=0;
      buffer->arrayPos++;
   }
}

void ow_bit_buffer_clear(ow_bit_buffer *buffer) {
   buffer->arrayPos=0;
   buffer->bytePos=0;
}

/************************************************************************
 * byte_buffer functions
 ************************************************************************/

ow_byte_buffer *ow_byte_buffer_create(apr_pool_t *pool) 
{
   ow_byte_buffer *b = apr_pcalloc(pool,sizeof(ow_byte_buffer));
   if( b!=NULL ) {
      b->size=0;
      b->head = NULL;
      b->last_alloc = 0;
      b->pool = pool;
   }
   return b;
}

apr_status_t ow_byte_buffer_append(ow_byte_buffer *b, ow_byte *buffer, ow_int size, ow_boolean flip) 
{
   
   // If we are starting from the back
   if(flip) {
      buffer += size;
   }
   
   while( size > 0 ) {
      
      int n,r,i;
      
      // Do we need to allocate another block?
      if( b->head == NULL || b->head->size==b->last_alloc ) {
		 int next_alloc;
         ow_byte_buffer_entry *entry = apr_pcalloc(b->pool, sizeof(ow_byte_buffer_entry));
         if( entry == NULL )
            return APR_ENOMEM;
         entry->next = b->head;
         b->head = entry;
         next_alloc = b->last_alloc==0 ? 512 : b->last_alloc*2;
         entry->values = apr_pcalloc(b->pool, next_alloc);
         if( entry->values == NULL )
            return APR_ENOMEM;
         b->last_alloc = next_alloc;
      }
      
      r = b->last_alloc - b->head->size;
      n = size > r ? r : size;
      
      if(flip) {
		 char *p = b->head->values + b->head->size;
         for( i=0; i < n; i++ ) {
			 buffer--;
            *p = *buffer;
            p++;
         }
      } else {
         memcpy(b->head->values+b->head->size,buffer,n);
         buffer += n;
      }
      b->head->size += n;
      b->size += n;
      size -= n;
   }
   
   return APR_SUCCESS;
}

apr_status_t ow_byte_array_read(ow_byte_array *array, ow_byte *dest, ow_int size, ow_boolean flip) {
   int i; 
   if( array->size < size ) {
      return APR_EOF;
   }
   if( flip ) {
      for( i=size-1; i >= 0; i-- ) {
		 dest[i] = *(array->values);
		 array->values++;         
      }
      array->size-=size;
   } else {
      memcpy(dest,array->values,size);
      array->size-=size;
      array->values+=size;
   }
   return APR_SUCCESS;
}

apr_status_t ow_byte_buffer_append_bit_buffer(ow_byte_buffer *buffer, ow_bit_buffer *bitbuffer) {
   apr_status_t rc = ow_byte_buffer_append_byte(buffer, bitbuffer->arrayLimit);
   if( rc != APR_SUCCESS )
      return rc;
   rc = ow_byte_buffer_append(buffer, bitbuffer->values, bitbuffer->arrayLimit, 0);
   if( rc != APR_SUCCESS )
      return rc;
   ow_bit_buffer_clear(bitbuffer);
   return rc;
}
apr_status_t ow_byte_array_read_bit_buffer(ow_byte_array *buffer, ow_bit_buffer **value, apr_pool_t *pool) {
   apr_status_t rc;
   ow_byte size;
   rc = ow_byte_array_read_byte(buffer, &size);
   if( rc != APR_SUCCESS ) 
      return rc;
   
   if( size >= 0 ) {
      
      if( buffer->size < size )
         return APR_EOF;
      
      *value = apr_pcalloc(pool, sizeof(ow_bit_buffer));
      if( *value == NULL )
         return APR_ENOMEM;
      
      (*value)->arrayLimit = size;
      return ow_byte_array_read(buffer, (*value)->values, size, 0);
      
   } else {
      *value=NULL;
   }
   return ow_byte_array_read(buffer,(ow_byte*)value, sizeof(ow_double), ENDIAN_FLIP);
}

apr_status_t ow_byte_buffer_entry_write(ow_byte_buffer_entry *b, ow_transport *transport) 
{
   if( b!=NULL ) {
      apr_status_t rc = ow_byte_buffer_entry_write(b->next, transport);
      if( rc != APR_SUCCESS ) 
         return rc;
      return ow_write(transport, b->values, b->size);
   }
   return APR_SUCCESS;
}
apr_status_t ow_byte_buffer_write(ow_byte_buffer *buffer, ow_transport *transport) {
   return ow_byte_buffer_entry_write(buffer->head, transport);
}


/************************************************************************
 * Functions that marshal/unmarshal primitives to the byte_buffer 
 ************************************************************************/

apr_status_t ow_byte_buffer_append_byte(ow_byte_buffer *buffer, ow_byte value) {
   return ow_byte_buffer_append(buffer,(ow_byte*)&value, sizeof(ow_byte), ENDIAN_FLIP);
}
apr_status_t ow_byte_array_read_byte(ow_byte_array *buffer, ow_byte *value) {   
   return ow_byte_array_read(buffer,(ow_byte*)value, sizeof(ow_byte), ENDIAN_FLIP); 
}

apr_status_t ow_byte_buffer_append_boolean(ow_byte_buffer *buffer, ow_boolean value) {
   return ow_byte_buffer_append(buffer,(ow_byte*)&value, sizeof(ow_boolean), ENDIAN_FLIP);
}
apr_status_t ow_byte_array_read_boolean(ow_byte_array *buffer, ow_boolean *value) {   
   return ow_byte_array_read(buffer,(ow_byte*)value, sizeof(ow_boolean), ENDIAN_FLIP);
}

apr_status_t ow_byte_buffer_append_char(ow_byte_buffer *buffer, ow_char value) {
   return ow_byte_buffer_append(buffer,(ow_byte*)&value, sizeof(ow_char), ENDIAN_FLIP);
}
apr_status_t ow_byte_array_read_char(ow_byte_array *buffer, ow_char *value) {   
   return ow_byte_array_read(buffer,(ow_byte*)value, sizeof(ow_char), ENDIAN_FLIP);
}

apr_status_t ow_byte_buffer_append_short(ow_byte_buffer *buffer, ow_short value) {
   return ow_byte_buffer_append(buffer,(ow_byte*)&value, sizeof(ow_short), ENDIAN_FLIP);
}
apr_status_t ow_byte_array_read_short(ow_byte_array *buffer, ow_short *value) {   
   return ow_byte_array_read(buffer,(ow_byte*)value, sizeof(ow_short), ENDIAN_FLIP);
}

apr_status_t ow_byte_buffer_append_int(ow_byte_buffer *buffer, ow_int value) {
   return ow_byte_buffer_append(buffer,(ow_byte*)&value, sizeof(ow_int), ENDIAN_FLIP);
}
apr_status_t ow_byte_array_read_int(ow_byte_array *buffer, ow_int *value) {   
   return ow_byte_array_read(buffer,(ow_byte*)value, sizeof(ow_int), ENDIAN_FLIP);
}

apr_status_t ow_byte_buffer_append_long(ow_byte_buffer *buffer, ow_long value) {
   return ow_byte_buffer_append(buffer,(ow_byte*)&value, sizeof(ow_long), ENDIAN_FLIP);
}
apr_status_t ow_byte_array_read_long(ow_byte_array *buffer, ow_long *value) {   
   return ow_byte_array_read(buffer,(ow_byte*)value, sizeof(ow_long), ENDIAN_FLIP);
}

apr_status_t ow_byte_buffer_append_float(ow_byte_buffer *buffer, ow_float value) {
   return ow_byte_buffer_append(buffer,(ow_byte*)&value, sizeof(ow_float), ENDIAN_FLIP);
}
apr_status_t ow_byte_array_read_float(ow_byte_array *buffer, ow_float *value) {   
   return ow_byte_array_read(buffer,(ow_byte*)value, sizeof(ow_float), ENDIAN_FLIP);
}

apr_status_t ow_byte_buffer_append_double(ow_byte_buffer *buffer, ow_double value) {
   return ow_byte_buffer_append(buffer,(ow_byte*)&value, sizeof(ow_double), ENDIAN_FLIP);
}
apr_status_t ow_byte_array_read_double(ow_byte_array *buffer, ow_double *value) {   
   return ow_byte_array_read(buffer,(ow_byte*)value, sizeof(ow_double), ENDIAN_FLIP);
}
