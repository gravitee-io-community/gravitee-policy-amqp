/**
 * Copyright (C) 2015 The Gravitee team (http://gravitee.io)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.gravitee.plugins.rabbitmq;

import io.vertx.amqpbridge.AmqpBridge;
import io.vertx.amqpbridge.AmqpBridgeOptions;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.eventbus.MessageProducer;
import io.vertx.core.json.JsonObject;

public class AmqpSender {
    public static void main(String[] args) throws Exception {

        AmqpBridgeOptions options = new AmqpBridgeOptions();
        options.setReplyHandlingSupport(true);
        AmqpBridge amqpBridge = AmqpBridge.create(Vertx.vertx(), options);


        amqpBridge.start("localhost", 5672, "test", "test", res -> {

            if (!res.succeeded()) {
                System.out.println("Bridge startup failed: " + res.cause());
                return;
            }

            System.out.println("Connected !");

            // Set up a producer using the bridge, send a message with it.
            MessageProducer<JsonObject> producer = amqpBridge.createProducer("/gravitee-api");


            try {
                JsonObject amqpMsgPayload = new JsonObject();
                JsonObject properties = new JsonObject();
                properties.put("reply_to", "/gravitee-reply");
                amqpMsgPayload.put("body", "myStringContent");
                amqpMsgPayload.put("properties", properties);

                //producer.deliveryOptions(new DeliveryOptions().addHeader("reply_to", "foobar"));
                producer.send(amqpMsgPayload, repsonse -> {
                    System.out.println(repsonse);
                });
            } catch (Exception e) {
                e.printStackTrace();
            }


        });
    }
}
