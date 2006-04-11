/**
 * 
 * Copyright 2005-2006 The Apache Software Foundation
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.activemq.perf;

import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
/**
 * @version $Revision: 1.3 $
 */
public class SlowConsumer extends PerfConsumer{
    public SlowConsumer(ConnectionFactory fac,Destination dest,String consumerName,boolean slowConsumer)
                    throws JMSException{
        super(fac,dest,consumerName);
    }

    public SlowConsumer(ConnectionFactory fac,Destination dest) throws JMSException{
        super(fac,dest,null);
    }

    public void onMessage(Message msg){
        super.onMessage(msg);
        try{
            Thread.sleep(10000);
        }catch(InterruptedException e){
            e.printStackTrace();
        }
    }
}