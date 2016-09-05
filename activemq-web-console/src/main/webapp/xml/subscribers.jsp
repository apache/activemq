<%@ page contentType="text/xml;charset=ISO-8859-1"%>
<%--
    Licensed to the Apache Software Foundation (ASF) under one or more
    contributor license agreements.  See the NOTICE file distributed with
    this work for additional information regarding copyright ownership.
    The ASF licenses this file to You under the Apache License, Version 2.0
    (the "License"); you may not use this file except in compliance with
    the License.  You may obtain a copy of the License at
   
    http://www.apache.org/licenses/LICENSE-2.0
   
    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
--%>
<subscribers>
<c:forEach items="${requestContext.brokerQuery.durableTopicSubscribers}" var="row">
<subscriber clientId="<c:out value="${row.clientId}"/>" 
            subscriptionName="<c:out value="${row.subscriptionName}"/>" 
            connectionId="<c:out value="${row.connectionId}"/>"
            destinationName="<c:out value="${row.destinationName}"/>" 
            selector="<c:out value="${row.selector}"/>" 
            active="yes" >
  <stats pendingQueueSize="<c:out value="${row.pendingQueueSize}"/>"
         dispatchedQueueSize="<c:out value="${row.dispatchedQueueSize}"/>"
         dispatchedCounter="<c:out value="${row.dispatchedCounter}"/>"
         enqueueCounter="<c:out value="${row.enqueueCounter}"/>"
         dequeueCounter="<c:out value="${row.dequeueCounter}"/>" />

</subscriber>
</c:forEach>

<c:forEach items="${requestContext.brokerQuery.inactiveDurableTopicSubscribers}" var="row">
<subscriber clientId="<c:out value="${row.clientId}"/>" 
            subscriptionName="<c:out value="${row.subscriptionName}"/>" 
            connectionId="<c:out value="${row.connectionId}"/>"
            destinationName="<c:out value="${row.destinationName}"/>" 
            selector="<c:out value="${row.selector}"/>" 
            active="no" >
  <stats pendingQueueSize="<c:out value="${row.pendingQueueSize}"/>"
         dispatchedQueueSize="<c:out value="${row.dispatchedQueueSize}"/>"
         dispatchedCounter="<c:out value="${row.dispatchedCounter}"/>"
         enqueueCounter="<c:out value="${row.enqueueCounter}"/>"
         dequeueCounter="<c:out value="${row.dequeueCounter}"/>"/>

</subscriber>
</c:forEach>


</subscribers>
