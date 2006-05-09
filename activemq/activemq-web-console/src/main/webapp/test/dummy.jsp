<html>
<head>
<title>Test Pages</title>
</head>
<body>

<h2>Test Pages</h2>

These pages are used to test out the environment and web framework.

<table class="autostripe">
<thead>
<tr>
<th colspan="2">Values</th>
</tr>
</thead>
<tbody>
<tr> 
  <td class="label">Broker type</td>
  <td>${requestContext.brokerQuery.broker.class}</td>
</tr>
<tr> 
  <td class="label">Managed broker</td>
  <td>${requestContext.brokerQuery.brokerAdmin.broker.class}</td>
</tr>
<tr> 
  <td class="label">Destinations</td>
  <td>${requestContext.brokerQuery.managedBroker.queueRegion.destinationMap}</td>
</tr>
</tbody>
</table>


</body>
</html>
	