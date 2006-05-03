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
<th>Messages Sent</th>
<th>Messages Received</th>
<th>Operations</th>
</tr>
</thead>
<tbody>
<c:forEach items="${requestContext.brokerQuery.topics}" var="row">
<tr>
<td><a href="send.jsp?JMSDestination=${row.name}&JMSDestinationType=topic">${row.name}</a></td>
<td>${row.consumerCount}</td>
<td>${row.enqueueCount}</td>
<td>${row.dequeueCount}</td>
<td>
    <a href="send.jsp?JMSDestination=${row.name}&JMSDestinationType=topic">Send To</a>
    <a href="deleteDestination.action?JMSDestination=${row.name}&JMSDestinationType=topic">Delete</a>
</td>
</tr>
</c:forEach>
</tbody>
</table>


</body>
</html>
	