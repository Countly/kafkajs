---
id: faq
title: FAQ
---

## Is KafkaJS used in production?

**Yes**. KafkaJS is used across hundreds of services running in production across businesses over the world.

## Why am I receiving messages for topics I'm not subscribed to?

If you are seeing the warning `[ConsumerGroup] Consumer group received unsubscribed topics`, the group's leader gave this member partitions of a topic it does not subscribe to. The member drops them, so nobody consumes those partitions until the next rebalance. The leader is one member of the group, and its assigner decides for every member. This warning and `RECEIVED_UNSUBSCRIBED_TOPICS` apply to the classic group protocol. Under the consumer protocol, names outside the round subscription are filtered with a debug message and no event or warning.

The assigners of this client never do that: `roundRobin` skips the members that do not subscribe to a partition's topic, `range` divides each topic among its subscribers only, and `sticky` and `cooperativeSticky` never hand a partition back to a previous owner that has left its topic. So the warning means that the leader runs a custom assigner or another client: an older KafkaJS (2.2.4 or earlier), or a Java consumer whose `sticky` or `cooperative-sticky` assignor hands such a partition back (Kafka 4.3's `AbstractStickyAssignor` does not check the subscription).

Members subscribe to different topics most often when a `groupId` is re-used across several applications or several different independent deployments of the same application, and briefly while a new version of an application that subscribes to a new topic is deployed.

Ensure that your `groupId` is not used by any other application. A simple way to verify this is to [describe the consumer group](Consuming.md#describe-group) and verify that there are no unexpected members.

Note: If you are switching from node-rdkafka, then this behavior is not the same. You may want to listen to the [RECEIVED_UNSUBSCRIBED_TOPICS](InstrumentationEvents.md#a-name-list-list-of-available-events) event and act accordingly.

## What does it mean to get `REBALANCE_IN_PROGRESS` errors?

This is a normal occurrence during, for example, deployments, and should resolve itself. However, if this continues to happen frequently when the group should be stable, it may indicate that your session timeout is too low, or that processing each message is taking too long.

Every instance of your application is a member of a consumer group, and is exclusively assigned one or more partitions to consume from. This means that that consumer is the only one within the consumer group that is allowed to consume from that partition. A rebalance means that this ownership is being re-assigned.

A rebalance will happen in a number of scenarios:

* A new member joins the consumer group
* A member leaves the consumer group (for example when shutting down)
* A member is considered dead by the group coordinator after no heartbeat arrives within `sessionTimeout`. Yielding asynchronous work is kept alive by the background heartbeat, while an event-loop-blocking handler can starve it. Processing is also bounded by `maxPollInterval`; increasing `sessionTimeout` or manually calling `heartbeat()` does not extend that deadline. The poll guard is global across all partitions, so progress on another partition can refresh it while one handler is stuck.
* Partitions have been added or removed from the topic

The rebalancing state is enforced on the broker side. When a consumer tries to commit offsets, the broker will respond with `REBALANCE_IN_PROGRESS`. Upon receiving that, the consumer group leader will receive a list of currently active members of the consumer group. Using the [partition assigner](Consuming.md#a-name-custom-partition-assigner-a-custom-partition-assigner) configured on the client, the consumer group leader will assign each partition to a consumer within the group and submit the assignment back to the group coordinator, which distributes the relevant assignments to the members of the consumer group. When this is done, the group is considered in-sync again and processing can continue.

## Can KafkaJS be used in a browser?

No - KafkaJS is a library for NodeJS and will not work in a browser context.

Communication with Kafka happens over a TCP socket. The NodeJS API used to create that socket is [`net.connect`](https://nodejs.org/api/net.html#net_net_connect)/[`tls.connect`](https://nodejs.org/api/tls.html#tls_tls_connect_options_callback). There is no equivalent API in browsers [as of yet](https://github.com/WICG/raw-sockets), which means that even if you run KafkaJS through a transpilation tool like [Browserify](http://browserify.org/), it cannot polyfill those modules.

For more information, see:

* [#508](https://github.com/tulios/kafkajs/issues/508#issuecomment-535382981)
* [#36](https://github.com/tulios/kafkajs/issues/36)
* [Custom Socket Factory](./Configuration.md#custom-socket-factory)

## Didn't find what you were looking for?

Please [open an issue](https://github.com/Countly/kafkajs/issues)
