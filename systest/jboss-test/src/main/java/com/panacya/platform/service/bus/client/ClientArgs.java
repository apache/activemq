/** 
 * 
 * Copyright 2004 Michael Gaffney
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
 * 
 **/
package com.panacya.platform.service.bus.client;

/**
 * @author <a href="mailto:michael.gaffney@panacya.com">Michael Gaffney </a>
 */
public class ClientArgs {
    private long timeout = -1;
    private String command;
    private String destination;

    private int noOfMessages = 1;

    public ClientArgs(String[] args) {
        /*
        int argCount = args.length;

        System.out.println(argCount);
        for(int i = 0 ; i < argCount; i++){
            System.out.println(args[i] + " ..........");
        }
         */
        switch (args.length) {
            case 4:
                setNoOfMessages(args[3]);
            case 3:
                setTimeout(args[2]);
            case 2:
                destination = args[1];
                command = args[0];
                break;
            default :
                printHelp();
        }
    }

    public String getCommand() {
        return command;
    }

    public String getDestination() {
        return destination;
    }

    public long getTimeout() {
        return timeout;
    }

    private void setTimeout(String timout) {
        if (!isEmpty(timout)) {
            try {
                timeout = Long.valueOf(timout).longValue();
            } catch (NumberFormatException e) {
                System.err.println(e.toString());
            }
        }
    }

    public int getNoOfMessages() {
        return noOfMessages;
    }

    public void setNoOfMessages(String count) {
        System.out.println("noOfMessage " + count);
        this.noOfMessages = Integer.parseInt(count);
    }

    private static boolean isEmpty(String value) {
        return value == null || "".equals(value.trim());
    }

    private static void printHelp() {
        System.out.println("JmsSimpleClient command(send | receive | send-receive) noOfMessages destination timeout");
    }
}    

