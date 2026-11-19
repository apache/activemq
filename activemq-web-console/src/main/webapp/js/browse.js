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

addEvent(window, 'load', (_event) => {
    for (const link of document.querySelectorAll('a.actionDelete')) {
        link.onclick = function(e) {
            return confirm('Are you sure you want to delete this message?');
        }
    }

    for (const link of document.querySelectorAll('a.actionRetry')) {
        link.onclick = function(e) {
            return confirm('Are you sure you want to retry this message on queue://' + link.dataset.queue + '?');
        }
    }
});
