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
$messages = 10000;
$size = 256;

$DATA = "abcdefghijklmnopqrstuvwxyz";
$body = "";
for($i=0; $i< $size; $i++) {
  $body .= $DATA[ $i % 26];
}

try {
  $url = 'tcp://'.$host.":".$port;
  $stomp = new Stomp($url, $user, $password);
  
  for($i=0; $i< $messages; $i++) {
    $stomp->send($destination, $body);
    if( $i%1000 == 0 ) {
      echo "Sent ".$i." messages\n";
    }
  }
  
  $stomp->send($destination, "SHUTDOWN");

} catch(StompException $e) {
  echo $e->getMessage();
}
