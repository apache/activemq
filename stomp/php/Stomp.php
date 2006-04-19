<?php
/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/* vim: set expandtab tabstop=3 shiftwidth=3: */

/**
 * A StompFrame are the messages that are sent and received on a StompConnection.
 *
 * @package Stomp
 * @author Hiram Chirino <hiram@hiramchirino.com>
 * @version $Revision$
 */
class StompFrame {
    var $command;
    var $headers;
    var $body;
    
    function StompFrame($command = null, $headers=null, $body=null) {
        $this->command = $command;
        $this->headers = $headers;
        $this->body = $body;
    }
}

/**
 * A Stomp Connection
 *
 * The class wraps around HTTP_Request providing a higher-level
 * API for performing multiple HTTP requests
 *
 * @package Stomp
 * @author Hiram Chirino <hiram@hiramchirino.com>
 * @version $Revision$
 */
class StompConnection {

    var $socket;

    function StompConnection($host, $port = 61613) {
        $this->socket = socket_create(AF_INET, SOCK_STREAM, 0) or die("Could not create socket\n");
        $result = socket_connect($this->socket, $host, $port) or die("Could not connect to server\n");
    }


    function connect($userName="", $password="") {
        $this->writeFrame( new StompFrame("CONNECT", array("login"=>$userName, "passcode"=> $password ) ) );
        return $this->readFrame();
    }

    function send($destination, $body, $properties=null) {
        $headers = array();
        if( isset($properties) ) {
            foreach ($properties as $name => $value) {
                $headers[$name] = $value;
            }
        }
        $headers["destination"] = $destination ;
        $this->writeFrame( new StompFrame("SEND", $headers, $body) );
    }
    
    function subscribe($destination, $properties=null) {
        $headers = array("ack"=>"client");
        if( isset($properties) ) {
            foreach ($properties as $name => $value) {
                $headers[$name] = $value;
            }
        }
        $headers["destination"] = $destination ;
        $this->writeFrame( new StompFrame("SUBSCRIBE", $headers) );
    }
    
    function unsubscribe($destination, $properties=null) {
        $headers = array();
        if( isset($properties) ) {
            foreach ($properties as $name => $value) {
                $headers[$name] = $value;
            }
        }
        $headers["destination"] = $destination ;
        $this->writeFrame( new StompFrame("UNSUBSCRIBE", $headers) );
    }

    function begin($transactionId=null) {
        $headers = array();
        if( isset($transactionId) ) {
            $headers["transaction"] = $transactionId;
        }
        $this->writeFrame( new StompFrame("BEGIN", $headers) );
    }
    
    function commit($transactionId=null) {
        $headers = array();
        if( isset($transactionId) ) {
            $headers["transaction"] = $transactionId;
        }
        $this->writeFrame( new StompFrame("COMMIT", $headers) );
    }

    function abort($transactionId=null) {
        $headers = array();
        if( isset($transactionId) ) {
            $headers["transaction"] = $transactionId;
        }
        $this->writeFrame( new StompFrame("ABORT", $headers) );
    }
    
    function acknowledge($messageId, $transactionId=null) {
        $headers = array();
        if( isset($transactionId) ) {
            $headers["transaction"] = $transactionId;
        }
        $headers["message-id"] = $messageId ;
        $this->writeFrame( new StompFrame("ABORT", $headers) );
    }
    
    function disconnect() {
        $this->writeFrame( new StompFrame("DISCONNECT") );
        socket_close($this->socket);
    }
    
    function writeFrame($stompFrame) {
        $data = $stompFrame->command . "\n";        
        if( isset($stompFrame->headers) ) {
            foreach ($stompFrame->headers as $name => $value) {
                $data .= $name . ": " . $value . "\n";
            }
        }
        $data .= "\n";
        if( isset($stompFrame->body) ) {
            $data .= $stompFrame->body;
        }
        $l1 = strlen($data);
        $data .= "\x00\n";
        $l2 = strlen($data);
        
        socket_write($this->socket, $data, strlen($data)) or die("Could not send stomp frame to server\n");
    }
    
    function readFrame() {

        $rc = socket_recv($this->socket, &$b, 1, 0);
        
        // I think this EOF
        if( $rc == 0 ) {
            return null;
        }
        // I think this is no data.
        if( $rc == false ) {
            return null;
        }       
        
        // Read until end of frame.
        while( ord($b) != 0  ) {

            $data .= $b;
            $t = ord($b);
                                
            $rc = socket_recv($this->socket,&$b,1,0);
            
            // I think this EOF
            if( $rc == 0 ) {
                return null;
            }
            
        }
        
        # Read in the \n that comes after the \0
        $rc = socket_recv($this->socket,&$b,1,0);
        if( $rc == 0 ) {
            return null;
        }
        if( ord($b) != 10 ) {
            return null;
        }

        list($header, $body) = explode("\n\n", $data, 2);
        $header = explode("\n", $header);
        $headers = array();
        
        $command = null;
        foreach ($header as $v) {
           if( isset($command) ) {
                list($name, $value) = explode(':', $v, 2);
                $headers[$name]=$value;
           } else {
                $command = $v;
           }
        }
        
        return new StompFrame($command, $headers, $body);       
    }
}

?>

