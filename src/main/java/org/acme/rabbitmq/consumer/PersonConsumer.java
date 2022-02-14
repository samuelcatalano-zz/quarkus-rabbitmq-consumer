package org.acme.rabbitmq.consumer;

import com.rabbitmq.client.*;
import io.quarkiverse.rabbitmqclient.RabbitMQClient;
import io.quarkus.runtime.StartupEvent;
import org.acme.rabbitmq.consumer.base.BaseConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

@ApplicationScoped
public class PersonConsumer extends BaseConsumer {

    private static final Logger log = LoggerFactory.getLogger(PersonConsumer.class);

    private static final String PRODUCER_NAME = "person";

    /**
     * Application startup event.
     * @param event the startup event
     */
    public void onApplicationStart(@Observes StartupEvent event) {
        consume();
    }

    /**
     * Consume messages from the person queue.
     */
    public void consume() {
        try {
            final List<RabbitMQClient> clients = getRegisteredClients();
            for (final RabbitMQClient client : clients) {
                setupQueues(client, PRODUCER_NAME);
                // register a consumer for messages
                channel.basicConsume(queue, true, new DefaultConsumer(channel) {
                    @Override
                    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
                    byte[] body) {
                        // just print the received message.
                        log.info(String.format("Received: %s", new String(body, StandardCharsets.UTF_8)));
                    }
                });
            }
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}