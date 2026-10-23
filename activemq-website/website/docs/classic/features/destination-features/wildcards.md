---
title: "Wildcards"
slug: /wildcards
---


We support destination wildcards to provide easy support for federated name hierarchies. This concept has been popular in financial market data for some time as a way of organizing events (such as price changes) into hierarchies and to use wildcards for easy subscription of the range of information you're interested in.

For example imagine you are sending price messages from a stock exchange feed. You might use some kind of destination such as

*   `PRICE.STOCK.NASDAQ.ORCL` to publish Oracle Corporation's price on NASDAQ and
*   `PRICE.STOCK.NYSE.IBM` to publish IBM's price on the New York Stock Exchange

A subscriber could then use exact destinations to subscribe to exactly the prices it requires. Or it could use wildcards to define hierarchical pattern matches to the destinations to subscribe from.

### Wildcards supported

We support the following wildcards which are fairly standard practice; wildcards are not part of the JMS specification so are custom enhancements.

*   `.` is used to separate names in a path
*   `*` is used to match any name in a path
*   `>` is used to recursively match any destination starting from this name

For example using the example above, these subscriptions are possible

Subscription

Meaning
```
`PRICE.>`
```
Any price for any product on any exchange
```
`PRICE.STOCK.>`
```
Any price for a stock on any exchange
```
`PRICE.STOCK.NASDAQ.*`
```
Any stock price on NASDAQ
```
`PRICE.STOCK.*.IBM`
```
Any IBM stock price on any exchange

`Note:** Don't specify any string after '>' on your wildcard expression, it will be ignored. E.g. using a wildcard `PRICE.>.IBM` will also match `PRICE.STOCK.NASDAQ.FB.` Using '>' really matches everything till the end of the destination name.

### Custom path separator

As of version 5.5.0 we support [plugin](interceptors) that allows clients to use customer path separator. So instead of
```
`FOO.BAR.*`
```
you can use
```
`FOO/BAR/*`
```
To
```
    <plugins>
       .....
       <destinationPathSeparatorPlugin/>
    </plugins>
```
ActiveMQ 3.2.1 Release 

A default path separator this plugin will use is `/`. You can customize it further using `pathSeparator` property.

