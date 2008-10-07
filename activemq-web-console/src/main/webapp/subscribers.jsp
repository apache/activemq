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
<html>
<head>
<title>Durable Topic Subscribers</title>
</head>
<body>

<form action="createSubscriber.action" method="get">
    <input type="hidden" name="JMSDestinationType" value="topic"/>

<table id="createSubscribers" class="sortable autostripe">
<thead>
<tr>
    <th colspan="4">Create Durable Topic Subscribers</th>
</tr>
</thead>
<tbody>

<tr>
	<td class="label">
	    <label name="clientId">Client ID</label>
	</td>
    <td>
        <input type="text" name="clientId" value=""/>
    </td>
    <td class="label">
        <label name="subscriberName">Subscriber Name</label>
    </td>
    <td>
        <input type="text" name="subscriberName" value=""/>
    </td>
</tr>
<tr>
    <td>
		<label name="JMSDestination">Topic Name</label>
    </td>
    <td>
		<input type="text" name="JMSDestination" value=""/>
    </td>
    <td>
        <label name="selector">JMS Selector</label>
    </td>
    <td>
        <input type="text" name="selector" value=""/>
    </td>
</tr>
<tr>
    <td colspan="4" align="center">
        <input type="submit" value="Create Durable Topic Subscriber"/>
    </td>
</tr>
</tbody>
</table>
</form>


<h2>Durable Topic Subscribers</h2>


<table id="topics" class="sortable autostripe">
<thead>
<tr>
<th>Client ID</th>
<th>Subscription Name</th>
<th>Connection ID</th>
<th>Destination</th>
<th>Selector</th>
<th>Pending Queue Size</th>
<th>Dispatched Queue Size</th>
<th>Dispatched Counter</th>
<th>Enqueue Counter</th>
<th>Dequeue Counter</th>
<th>Operations</th>
</tr>
</thead>
<tbody>
<c:forEach items="${requestContext.brokerQuery.durableTopicSubscribers}" var="row">
<tr>
<td><form:tooltip text="${row.clientId}" length="10"/></td>
<td><form:tooltip text="${row.subscriptionName}" length="10"/></td>
<td><form:tooltip text="${row.connectionId}" length="10"/></td>
<td><form:tooltip text="${row.destinationName}" length="10"/></td>
<td>${row.selector}</td>
<td>${row.pendingQueueSize}</td>
<td>${row.dispatchedQueueSize}</td>
<td>${row.dispachedCounter}</td>
<td>${row.enqueueCounter}</td>
<td>${row.dequeueCounter}</td>
<td>
    <a href="deleteSubscriber.action?clientId=${row.clientId}&subscriberName=${row.subscriptionName}">Delete</a>
</td>
</tr>
</c:forEach>
</tbody>
</table>


</body>
</html>
	
