package org.acme.rabbitmq.consumer.base;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import io.quarkiverse.rabbitmqclient.NamedRabbitMQClient;
import io.quarkiverse.rabbitmqclient.RabbitMQClient;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

public abstract class BaseConsumer {

    protected String exchange = "%s-exchange";
    protected String queue = "%s-queue";

    protected List<RabbitMQClient> registeredClients;
    protected Channel channel;

    @Inject
    @NamedRabbitMQClient("foo")
    protected RabbitMQClient fooClient;

    @Inject
    @NamedRabbitMQClient("bar")
    protected RabbitMQClient barClient;

    /**
     * Post constructor to register the clients.
     */
    @PostConstruct
    protected void init() {
        registeredClients = List.of(fooClient, barClient);
    }

    public abstract void consume();

    /**
     * Set up the queues for the consumer.
     */
    protected void setupQueues(final RabbitMQClient client, final String producerName) {
        try {
            // create a connection
            final Connection connection = client.connect();;

            // create a channel
            channel = connection.createChannel();
            // declare exchanges and queues

            exchange = String.format(exchange, producerName);
            queue = String.format(queue, producerName);

            channel.exchangeDeclare(exchange, BuiltinExchangeType.TOPIC, true);
            channel.queueDeclare(queue, true, false, false, null);
            channel.queueBind(queue, exchange, "");
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Returns the registered clients.
     * @return the registered clients
     */
    public List<RabbitMQClient> getRegisteredClients() {
        return registeredClients;
    }
}
