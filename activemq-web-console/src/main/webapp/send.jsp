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
<title>Send Messages</title>
</head>
<body>

<h2>Send a JMS Message</h2>

<form action="sendMessage.action" method="post">
<input type="hidden" name="secret" value="<c:out value='${sessionScope["secret"]}'/>"/>

<table id="headers" class="autostripe">
<thead>
<tr>
<th colspan="4">Message Header</th>
</tr>
</thead>

<tbody>
<tr>
	<td class="label">
	    <label for="JMSDestination">Destination</label>
	</td>
	<td>
	    <form:text name="JMSDestination" defaultValue="foo.bar" />
	</td>
	<td class="label">
	    <label for="queue">Queue or Topic</label>
	</td>
	<td>
	    <select name="JMSDestinationType">
	      <form:option name="JMSDestinationType" value="queue" label="Queue"/>
	      <form:option name="JMSDestinationType" value="topic" label="Topic"/>
	   </select>
	</td>
</tr>
<tr>
	<td class="label">
	    <label for="JMSCorrelationID">Correlation ID</label>
	</td>
	<td>
	    <form:text name="JMSCorrelationID"/>
	</td>
	<td class="label">
	    <label for="JMSPersistent">Persistent Delivery</label>
	</td>
	<td>
	    <form:checkbox name="JMSPersistent"/>
	</td>
</tr>
<tr>
	<td class="label">
	    <label for="JMSReplyTo">Reply To</label>
	</td>
	<td>
	    <form:text name="JMSReplyTo"/>
	</td>
	<td class="label">
	    <label for="JMSPriority">Priority</label>
	</td>
	<td>
	    <form:text name="JMSPriority"/>
	</td>
</tr>
<tr>
	<td class="label">
	    <label for="JMSType">Type</label>
	</td>
	<td>
	    <form:text name="JMSType"/>
	</td>
	<td class="label">
	    <label for="JMSTimeToLive">Time to live</label>
	</td>
	<td>
	    <form:text name="JMSTimeToLive"/>
	</td>
</tr>
<tr>
	<td class="label">
	    <label for="JMSXGroupID">Message Group</label>
	</td>
	<td>
	    <form:text name="JMSXGroupID"/>
	</td>
	<td class="label">
	    <label for="JMSXGroupSeq">Message Group Sequence Number</label>
	</td>
	<td>
	    <form:text name="JMSXGroupSeq"/>
	</td>
</tr>
<tr>
	<td class="label">
	    <label for="JMSMessageCount">Number of messages to send</label>
	</td>
	<td>
	    <form:text name="JMSMessageCount" defaultValue="1"/>
	</td>
	<td class="label">
	    <label for="JMSMessageCountHeader">Header to store the counter</label>
	</td>
	<td>
	    <form:text name="JMSMessageCountHeader" defaultValue="JMSXMessageCounter"/>
	</td>
</tr>
<tr>
 <td colspan="4" align="center">
     <input type="submit" value="Send"/>
     <input type="reset"/>
 </td>
</tr>
<tr>
	<th colspan="4" class="label">
	    <label for="text">Message body</label>
	</th>
</tr>
<tr>
	<td colspan="4">
	    <textarea name="JMSText" rows="30" cols="80">Enter some text here for the message body...</textarea>
	</td>
</tr>
</tbody>
</table>

</form>

</body>
</html>
