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

 
#ifndef AMQCS_H
#define AMQCS_H

#include "ow.h"
#include "ow_commands_v1.h"

#ifdef __cplusplus
extern "C" {
#endif /* __cplusplus */

typedef struct amqcs_connection amqcs_connection;

typedef struct amqcs_connect_options {
   ow_char hostname[255];
   ow_int  port;
   ow_char userId[255];
   ow_char password[255];
   ow_char clientId[255];
   ow_boolean useAsyncSend;
   
   ow_byte reserved[1000];
} amqcs_connect_options;
   
apr_status_t amqcs_send(amqcs_connection *connection, ow_ActiveMQDestination *dest,  ow_ActiveMQMessage *message, ow_int deliveryMode, ow_int priority, ow_long timeToLive, apr_pool_t *pool);
apr_status_t amqcs_disconnect(amqcs_connection **connection);
apr_status_t amqcs_connect(amqcs_connection **conn, amqcs_connect_options *options, apr_pool_t *pool);

#ifdef __cplusplus
}
#endif

#endif  /* ! AMQCS_H */