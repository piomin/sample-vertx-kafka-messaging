package pl.piomin.services.vertx.order;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import pl.piomin.services.vertx.order.model.Order;
import pl.piomin.services.vertx.order.model.OrderStatus;

public class AllOrderProcessorVerticle extends AbstractVerticle {

	private static final Logger LOGGER = LoggerFactory.getLogger(AllOrderProcessorVerticle.class);

	public static void main(String[] args) {
		Vertx vertx = Vertx.vertx();
		vertx.deployVerticle(new AllOrderProcessorVerticle());
	}

	@Override
	public void start() throws Exception {
		Properties config = new Properties();
		config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.99.100:9092");
		config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		config.put(ConsumerConfig.GROUP_ID_CONFIG, "all-orders");
		config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		KafkaConsumer<String, String> consumer = KafkaConsumer.create(vertx, config);
		consumer.subscribe("orders-out", ar -> {
			if (ar.succeeded()) {
				LOGGER.info("Subscribed");
			} else {
				LOGGER.error("Could not subscribe: err={}", ar.cause().getMessage());
			}
		});

		consumer.handler(record -> {
			LOGGER.info("Processing: key={}, value={}, partition={}, offset={}", record.key(), record.value(), record.partition(), record.offset());
			Order order = Json.decodeValue(record.value(), Order.class);
			order.setStatus(OrderStatus.DONE);
			LOGGER.info("Order processed: id={}, price={}", order.getId(), order.getPrice());
		});
	}

}
