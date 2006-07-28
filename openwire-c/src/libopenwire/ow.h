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


#ifndef AMQC_H
#define AMQC_H

#include "apr_general.h"
#include "apr_network_io.h"

#ifdef __cplusplus
extern "C" {
#endif /* __cplusplus */

/************************************************************************
 * Map the ow primitive types to the java counterparts.
 ************************************************************************/
typedef char            ow_boolean;
typedef char            ow_byte;
typedef char            ow_char;
typedef apr_int16_t     ow_short;
typedef apr_int32_t     ow_int;
typedef apr_int64_t     ow_long;
typedef float           ow_float;
typedef double          ow_double;

/************************************************************************
 * Helpers to handle endian conversions.
 ************************************************************************/
#if APR_IS_BIGENDIAN
#define ENDIAN_FLIP 0
#define ENDIAN_FLIP_INT(x) x
#else
#define ENDIAN_FLIP 1
#define ENDIAN_FLIP_INT(x) \
        ( x << 24 ) & 0xFF000000 | \
		( x << 8  ) & 0x00FF0000 | \
		( x >> 8  ) & 0x0000FF00 | \
		( x >> 24 ) & 0x000000FF
#endif

typedef struct ow_transport {
   apr_socket_t *socket;
   apr_sockaddr_t *local_sa;
   char *local_ip;
   apr_sockaddr_t *remote_sa;
   char *remote_ip;	
} ow_transport;

typedef struct ow_byte_array {
   ow_int size;
   ow_byte *values;
} ow_byte_array;

typedef struct ow_string {
   ow_int size;
   ow_char *values;
} ow_string;

typedef struct ow_throwable {
   struct ow_string errorClass;
   struct ow_string message;
} ow_throwable;

typedef struct ow_DataStructure {
   ow_short structType;
} ow_DataStructure;

typedef struct ow_DataStructure_array {
   ow_int size;
   struct ow_DataStructure **values;
} ow_DataStructure_array;

typedef struct ow_bit_buffer {
   ow_short arrayLimit;
   ow_short arrayPos;
   ow_short bytePos;
   ow_byte values[200];
} ow_bit_buffer;

typedef struct ow_byte_buffer_entry {
   struct ow_byte_buffer_entry *next;
   ow_int size;
   ow_byte *values;
} ow_byte_buffer_entry;

typedef struct ow_byte_buffer {
   ow_int size;
   ow_int last_alloc;
   apr_pool_t *pool;
   struct ow_byte_buffer_entry *head;
} ow_byte_buffer;


/************************************************************************
 * The transport interface related functions
 ************************************************************************/
apr_status_t ow_connect(ow_transport **transport_ref, const char *hostname, int port, apr_pool_t *pool);
apr_status_t ow_disconnect(ow_transport **transport_ref);
apr_status_t ow_write(ow_transport *transport, const ow_byte *data, apr_size_t size);
apr_status_t ow_read(ow_transport *transport, ow_byte *data, apr_size_t size);
apr_status_t ow_write_command(ow_transport *transport, ow_DataStructure *object);
apr_status_t ow_read_command(ow_transport *transport, ow_DataStructure **object, apr_pool_t *pool);

/************************************************************************
 * The primitive handling functions.
 ************************************************************************/
ow_byte_array *ow_byte_array_create(apr_pool_t *pool, apr_size_t size);
ow_byte_array *ow_byte_array_create_with_data(apr_pool_t *pool, apr_size_t size, ow_byte *data);
ow_string *ow_string_create(apr_pool_t *pool, apr_size_t size);
ow_string *ow_string_create_from_cstring(apr_pool_t *pool, const ow_char *source);
ow_DataStructure_array *ow_DataStructure_array_create(apr_pool_t *pool, apr_size_t size);
ow_throwable *ow_throwable_create(apr_pool_t *pool);

/************************************************************************
 * The ow_bit_buffer and ow_byte_buffer related functions
 ************************************************************************/
ow_bit_buffer *ow_bit_buffer_create(apr_pool_t *pool);

ow_boolean ow_bit_buffer_read(ow_bit_buffer *buffer);
void ow_bit_buffer_append(ow_bit_buffer *buffer, ow_boolean value);
void ow_bit_buffer_clear(ow_bit_buffer *buffer);

ow_byte_buffer *ow_byte_buffer_create(apr_pool_t *pool);

apr_status_t ow_byte_buffer_entry_write(ow_byte_buffer_entry *b, ow_transport *transport);
apr_status_t ow_byte_buffer_write(ow_byte_buffer *buffer, ow_transport *transport);

apr_status_t ow_byte_buffer_append_bit_buffer(ow_byte_buffer *buffer, ow_bit_buffer *bitbuffer);
apr_status_t ow_byte_array_read_bit_buffer(ow_byte_array *buffer, ow_bit_buffer **value, apr_pool_t *pool);

/************************************************************************
 * The primitive marshalling functions.
 ************************************************************************/
apr_status_t ow_byte_buffer_append(ow_byte_buffer *b, ow_byte *buffer, ow_int size, ow_boolean flip);
apr_status_t ow_byte_array_read(ow_byte_array *array, ow_byte *dest, ow_int size, ow_boolean flip);

apr_status_t ow_byte_buffer_append_byte(ow_byte_buffer *buffer, ow_byte value);
apr_status_t ow_byte_array_read_byte(ow_byte_array *buffer, ow_byte *value);   

apr_status_t ow_byte_buffer_append_boolean(ow_byte_buffer *buffer, ow_boolean value);
apr_status_t ow_byte_array_read_boolean(ow_byte_array *buffer, ow_boolean *value);   

apr_status_t ow_byte_buffer_append_char(ow_byte_buffer *buffer, ow_char value);
apr_status_t ow_byte_array_read_char(ow_byte_array *buffer, ow_char *value);   

apr_status_t ow_byte_buffer_append_short(ow_byte_buffer *buffer, ow_short value);
apr_status_t ow_byte_array_read_short(ow_byte_array *buffer, ow_short *value);   

apr_status_t ow_byte_buffer_append_int(ow_byte_buffer *buffer, ow_int value);
apr_status_t ow_byte_array_read_int(ow_byte_array *buffer, ow_int *value);   

apr_status_t ow_byte_buffer_append_long(ow_byte_buffer *buffer, ow_long value);
apr_status_t ow_byte_array_read_long(ow_byte_array *buffer, ow_long *value);   

apr_status_t ow_byte_buffer_append_float(ow_byte_buffer *buffer, ow_float value);
apr_status_t ow_byte_array_read_float(ow_byte_array *buffer, ow_float *value);   

apr_status_t ow_byte_buffer_append_double(ow_byte_buffer *buffer, ow_double value);
apr_status_t ow_byte_array_read_double(ow_byte_array *buffer, ow_double *value);   

/************************************************************************
 * The base command marshalling functions
 ************************************************************************/
apr_status_t ow_marshal1_nested_object(ow_bit_buffer *bitbuffer, ow_DataStructure *object);
apr_status_t ow_marshal2_nested_object(ow_byte_buffer *buffer, ow_bit_buffer *bitbuffer, ow_DataStructure *object);
apr_status_t ow_unmarshal_nested_object(ow_byte_array *data, ow_bit_buffer *bitbuffer, ow_DataStructure **object, apr_pool_t *pool);

apr_status_t ow_marshal1_cached_object(ow_bit_buffer *bitbuffer, ow_DataStructure *object);
apr_status_t ow_marshal2_cached_object(ow_byte_buffer *buffer, ow_bit_buffer *bitbuffer, ow_DataStructure *object);
apr_status_t ow_unmarshal_cached_object(ow_byte_array *data, ow_bit_buffer *bitbuffer, ow_DataStructure **object, apr_pool_t *pool);

apr_status_t ow_marshal1_string(ow_bit_buffer *buffer, ow_string *value);
apr_status_t ow_marshal2_string(ow_byte_buffer *buffer, ow_bit_buffer *bitbuffer, ow_string *value);
apr_status_t ow_unmarshal_string(ow_byte_array *buffer, ow_bit_buffer *bitbuffer, ow_string **value, apr_pool_t *pool);

apr_status_t ow_marshal1_throwable(ow_bit_buffer *buffer, ow_throwable *value);
apr_status_t ow_marshal2_throwable(ow_byte_buffer *buffer, ow_bit_buffer *bitbuffer, ow_throwable *value);
apr_status_t ow_unmarshal_throwable(ow_byte_array *buffer, ow_bit_buffer *bitbuffer, ow_throwable **value, apr_pool_t *pool);

apr_status_t ow_marshal2_byte_array_const_size(ow_byte_buffer *buffer, ow_byte_array *value, ow_int size);
apr_status_t ow_marshal2_byte_array(ow_byte_buffer *buffer, ow_bit_buffer *bitbuffer, ow_byte_array *value);
apr_status_t ow_unmarshal_byte_array(ow_byte_array *buffer,  ow_bit_buffer *bitbuffer, ow_byte_array **value, apr_pool_t *pool);
apr_status_t ow_unmarshal_byte_array_const_size(ow_byte_array *buffer, ow_byte_array **value, ow_int size, apr_pool_t *pool);

apr_status_t ow_marshal1_DataStructure(ow_bit_buffer *buffer, ow_DataStructure *object);
apr_status_t ow_marshal2_DataStructure(ow_byte_buffer *buffer, ow_bit_buffer *bitbuffer, ow_DataStructure *object);
apr_status_t ow_unmarshal_DataStructure(ow_byte_array *buffer, ow_bit_buffer *bitbuffer, ow_DataStructure *object,  apr_pool_t *pool);

apr_status_t ow_marshal1_DataStructure_array_const_size(ow_bit_buffer *buffer, ow_DataStructure_array *value, ow_int size);
apr_status_t ow_marshal2_DataStructure_array_const_size(ow_byte_buffer *buffer, ow_bit_buffer *bitbuffer, ow_DataStructure_array *value, ow_int size);
apr_status_t ow_unmarshal_DataStructure_array_const_size(ow_byte_array *buffer, ow_bit_buffer *bitbuffer, ow_DataStructure_array **value, ow_int size, apr_pool_t *pool);

apr_status_t ow_marshal1_DataStructure_array(ow_bit_buffer *buffer, ow_DataStructure_array *value);
apr_status_t ow_marshal2_DataStructure_array(ow_byte_buffer *buffer, ow_bit_buffer *bitbuffer, ow_DataStructure_array *value);
apr_status_t ow_unmarshal_DataStructure_array(ow_byte_array *buffer, ow_bit_buffer *bitbuffer, ow_DataStructure_array **value, apr_pool_t *pool);

ow_DataStructure *ow_create_object(ow_byte type, apr_pool_t *pool);
apr_status_t ow_marshal1_object(ow_bit_buffer *buffer, ow_DataStructure *object);
apr_status_t ow_marshal2_object(ow_byte_buffer *buffer, ow_bit_buffer *bitbuffer, ow_DataStructure *object);
apr_status_t ow_unmarshal_object(ow_byte_array *buffer, ow_bit_buffer *bitbuffer, ow_DataStructure *object, apr_pool_t *pool);

apr_status_t ow_marshal1_long(ow_bit_buffer *buffer, ow_long value);
apr_status_t ow_marshal2_long(ow_byte_buffer *buffer, ow_bit_buffer *bitbuffer, ow_long value);
apr_status_t ow_unmarshal_long(ow_byte_array *buffer, ow_bit_buffer *bitbuffer, ow_long *value, apr_pool_t *pool);
   
#ifdef __cplusplus
}
#endif

#endif  /* ! AMQC_H */
