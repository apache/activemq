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
package org.apache.activemq.replica;

import org.apache.activemq.broker.region.DestinationFilter;
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.broker.region.Topic;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

public class DestinationExtractorTest {

    @Test
    public void extractQueueFromQueue() {
        Queue queue = mock(Queue.class);
        Queue result = DestinationExtractor.extractQueue(queue);

        assertThat(result).isEqualTo(queue);
    }

    @Test
    public void extractQueueFromDestinationFilter() {
        Queue queue = mock(Queue.class);
        Queue result = DestinationExtractor.extractQueue(new DestinationFilter(queue));

        assertThat(result).isEqualTo(queue);
    }

    @Test
    public void extractNullFromNonQueue() {
        Topic topic = mock(Topic.class);
        Queue result = DestinationExtractor.extractQueue(topic);

        assertThat(result).isNull();
    }
}
