RabbitMQ Sender
=====

The sender does not use [RabbitMQ Publisher Confirms](https://www.rabbitmq.com/confirms.html),
so messages considered sent may not necessarily be received by consumers in case of RabbitMQ failure.

For thread safety, a new channel is created for every publish.
