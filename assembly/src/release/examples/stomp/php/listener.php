<?php
/*
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

$user = getenv("ACTIVEMQ_USER"); 
if( !$user ) $user = "admin";

$password = getenv("ACTIVEMQ_PASSWORD");
if( !$password ) $password = "password";

$host = getenv("ACTIVEMQ_HOST");
if( !$host ) $host = "localhost";

$port = getenv("ACTIVEMQ_PORT");
if( !$port ) $port = 61613;

$destination  = '/topic/event';


function now() { 
  list($usec,$sec) = explode(' ', microtime());
  return ((float)$usec + (float)$sec);
}

try {
  $url = 'tcp://'.$host.":".$port;
  $stomp = new Stomp($url, $user, $password);
  $stomp->subscribe($destination);
  
  $start = now();
  $count = 0;
  echo "Waiting for messages...\n";
  while(true) {
    $frame = $stomp->readFrame();
    if( $frame ) {
      if( $frame->command == "MESSAGE" ) {
        if($frame->body == "SHUTDOWN") {
          $diff = round((now()-$start),2);
          echo("Received ".$count." in ".$diff." seconds\n");
          break;
        } else {
          if(  $count==0 ) {
            $start = now();
          }
          if( $count%1000 == 0 ) {
            echo "Received ".$count." messages\n";
          }
          $count++;
        }
      } else {
        echo "Unexpected frame.\n";
        var_dump($frame);
      }
    }
  }

} catch(StompException $e) {
  echo $e->getMessage();
}
