package pl.piomin.services.vertx.order;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.ResponseContentTypeHandler;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import io.vertx.kafka.client.serialization.JsonObjectSerializer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.piomin.services.vertx.order.model.Order;
import pl.piomin.services.vertx.order.model.OrderStatus;

import java.util.Properties;

public class OrderVerticle extends AbstractVerticle {
private static final Logger LOGGER = LoggerFactory.getLogger(OrderVerticle.class);

   public static void main(String[] args) {
       Vertx vertx = Vertx.vertx();
       vertx.deployVerticle(new OrderVerticle());
   }

   @Override
   public void start() throws Exception {
       Properties config = new Properties();
       config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.99.100:9092");
       config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
       config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonObjectSerializer.class);
       config.put(ProducerConfig.ACKS_CONFIG, "1");
       KafkaProducer<String, JsonObject> producer = KafkaProducer.create(vertx, config);
       producer
         .partitionsFor("orders-out")
         .onSuccess(list -> list.forEach(p -> LOGGER.info("Partition: id={}, topic={}", p.getPartition(), p.getTopic())))
         .onFailure(err -> LOGGER.error("Cannot fetch partitions", err));

       Router router = Router.router(vertx);
       router.route("/order/*").handler(ResponseContentTypeHandler.create());
       router.route(HttpMethod.POST, "/order").handler(BodyHandler.create());
       router.post("/order").produces("application/json").handler(rc -> {
           Order o = Json.decodeValue(rc.getBodyAsString(), Order.class);
           KafkaProducerRecord<String, JsonObject> record = KafkaProducerRecord.create("orders-out", null, rc.getBodyAsJson(), o.getType().ordinal());
           producer.send(record)
             .onSuccess(md -> {
                 LOGGER.info("Record sent: msg={}, destination={}, partition={}, offset={}", record.value(), md.getTopic(), md.getPartition(), md.getOffset());
                 o.setId(md.getOffset());
                 o.setStatus(OrderStatus.PROCESSING);
                 rc.response().end(Json.encodePrettily(o));
             })
             .onFailure(err -> {
                 LOGGER.error("Error sending to topic", err);
                 o.setStatus(OrderStatus.REJECTED);
                 rc.response().end(Json.encodePrettily(o));
             });
       });
       vertx.createHttpServer().requestHandler(router).listen(8090);

    }

}
