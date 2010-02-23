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
<title>Topics</title>
</head>
<body>

<div>
<form action="createDestination.action" method="get">
    <input type="hidden" name="JMSDestinationType" value="topic"/>

    <label name="destination">Topic Name</label>
    <input type="text" name="JMSDestination" value=""/>

    <input type="submit" value="Create"/>
</form>
</div>

<h2>Topics</h2>

<table id="topics" class="sortable autostripe">
<thead>
<tr>
<th>Name</th>
<th>Number Of Consumers</th>
<th>Messages Enqueued</th>
<th>Messages Dequeued</th>
<th>Operations</th>
</tr>
</thead>
<tbody>
<c:forEach items="${requestContext.brokerQuery.topics}" var="row">
<tr>
<td><a href="send.jsp?JMSDestination=<c:out value="${row.name}" />&JMSDestinationType=topic"><form:tooltip text="${row.name}" length="50"/></a></td>
<td>${row.consumerCount}</td>
<td>${row.enqueueCount}</td>
<td>${row.dequeueCount}</td>
<td>
    <a href="send.jsp?JMSDestination=<c:out value="${row.name}" />&JMSDestinationType=topic">Send To</a>
    <a href="deleteDestination.action?JMSDestination=<c:out value="${row.name}" />&JMSDestinationType=topic">Delete</a>
</td>
</tr>
</c:forEach>
</tbody>
</table>


</body>
</html>
	
