/**
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
package org.apache.activemq.broker.scheduler;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.kahadb.util.ByteSequence;


public class JobImpl implements Job {
    private final JobLocation location;
    private final byte[] payload;
    
    protected JobImpl(JobLocation location,ByteSequence bs) {
        this.location=location;
        this.payload = new byte[bs.getLength()];
        System.arraycopy(bs.getData(), bs.getOffset(), this.payload, 0, bs.getLength());
    }

    public String getJobId() {
        return this.location.getJobId();
    }

    public byte[] getPayload() {
       return this.payload;
    }

    public long getPeriod() {
       return this.location.getPeriod();
    }

    public int getRepeat() {
       return this.location.getRepeat();
    }

    public long getStart() {
       return this.location.getStart();
    }

    public String getCronEntry() {
        return this.location.getCronEntry();
    }
    
    

    public String getNextExecutionTime() {
        // TODO Auto-generated method stub
        return null;
    }

    public String getStartTime() {
        return JobImpl.getDateTime(getStart());
    }
    
   public static long getDataTime(String value) throws Exception {
        DateFormat dfm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
     
        Date date = dfm.parse(value);
        return date.getTime();
    }
    
    public static String getDateTime(long value) {
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = new Date(value);
        return dateFormat.format(date);
    }

    
    

}
