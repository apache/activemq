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
package org.apache.activemq.tool;

import javax.jms.JMSException;

public interface PerfEventListener {
    public void onConfigStart(PerfMeasurable client);
    public void onConfigEnd(PerfMeasurable client);
    public void onPublishStart(PerfMeasurable client);
    public void onPublishEnd(PerfMeasurable client);
    public void onConsumeStart(PerfMeasurable client);
    public void onConsumeEnd(PerfMeasurable client);
    public void onJMSException(PerfMeasurable client, JMSException e);
    public void onException(PerfMeasurable client, Exception e);
}
