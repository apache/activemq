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
#include "amqcs.h"

int die(int exitCode, const char *message, apr_status_t reason) {
    char msgbuf[80];
	apr_strerror(reason, msgbuf, sizeof(msgbuf));
	fprintf(stderr, "%s: %s (%d)\n", message, msgbuf, reason);
	exit(exitCode);
	return reason;
}

static void terminate(void)
{
   apr_terminate();
}

int main(int argc, char *argv[])
{
   apr_status_t rc;
   amqcs_connection *connection;
   apr_pool_t *pool;
   amqcs_connect_options connect_options;
   
   memset(&connect_options, 0, sizeof(connect_options));
   strcpy(connect_options.hostname, "127.0.0.1");
	connect_options.port = 61616;
   
   setbuf(stdout, NULL);
   
   rc = apr_initialize();
	if( rc!=APR_SUCCESS ) 
		return rc;   
   atexit(terminate);	

   rc = apr_pool_create(&pool, NULL);
	rc==APR_SUCCESS || die(-2, "Could not allocate pool", rc);
   
   fprintf(stdout, "Connecting......");
   rc=amqcs_connect( &connection, &connect_options, pool);
	rc==APR_SUCCESS || die(-2, "Could not connect", rc);
   fprintf(stdout, "OK\n");
      
   fprintf(stdout, "Sending message.");
   {
      char *buffer = "Hello World!";
      ow_ActiveMQQueue *dest;
	  ow_ActiveMQTextMessage *message = ow_ActiveMQTextMessage_create(pool);
      message->content = ow_byte_array_create_with_data(pool,sizeof(buffer),buffer);
      dest = ow_ActiveMQQueue_create(pool);
      dest->physicalName = ow_string_create_from_cstring(pool,"TEST.QUEUE");         
      rc = amqcs_send(connection, (ow_ActiveMQDestination*)dest, (ow_ActiveMQMessage*)message, 1,4,0,pool);
      rc==APR_SUCCESS || die(-2, "Could not send message", rc);
   }  
   fprintf(stdout, "OK\n");
   
   fprintf(stdout, "Disconnecting...");
	rc=amqcs_disconnect(&connection); 
	rc==APR_SUCCESS || die(-2, "Could not disconnect", rc);
   fprintf(stdout, "OK\n");
   
   apr_pool_destroy(pool);	
   
   return 0;
}
