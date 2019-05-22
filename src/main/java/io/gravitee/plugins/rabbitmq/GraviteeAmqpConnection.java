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

import io.gravitee.gateway.api.ExecutionContext;
import io.gravitee.gateway.api.buffer.Buffer;
import io.gravitee.gateway.api.handler.Handler;
import io.gravitee.gateway.api.proxy.ProxyConnection;
import io.gravitee.gateway.api.proxy.ProxyResponse;
import io.gravitee.gateway.api.stream.WriteStream;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.MessageProducer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.amqp.AmqpClient;
import io.vertx.ext.amqp.AmqpClientOptions;
import io.vertx.ext.amqp.AmqpConnection;
import io.vertx.ext.amqp.AmqpMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class GraviteeAmqpConnection implements ProxyConnection {

    private static final Logger logger = LoggerFactory.getLogger(GraviteeAmqpConnection.class);

    //private AmqpBridge amqpBridge;
    private AmqpClient client;

    private Buffer content;

    private Handler<ProxyResponse> responseHandler;

    private AmqpPolicyConfiguration configuration;

    GraviteeAmqpConnection(ExecutionContext executionContext, AmqpPolicyConfiguration configuration) {
        this.configuration = configuration;
        Vertx vertx = executionContext.getComponent(Vertx.class);
        AmqpClientOptions options = new AmqpClientOptions()
                .setHost(configuration.getAmqpServerHostname())
                .setPort(configuration.getAmqpServerPort())
                .setUsername(configuration.getAmqpServerUsername())
                .setPassword(configuration.getAmqpServerPassword());

        this.client = AmqpClient.create(vertx, options);
    }

    @Override
    public WriteStream<Buffer> write(Buffer chunk) {
        if (content == null) {
            content = Buffer.buffer();
        }
        content.appendBuffer(chunk);
        return this;
    }

    @Override
    public void end() {

        logger.info("Sending connecting to amqp://{}:{}@{}:{}...",
                configuration.getAmqpServerUsername(),
                configuration.getAmqpServerPassword(),
                configuration.getAmqpServerHostname(),
                configuration.getAmqpServerPort());

        // Start the bridge, then use the event loop thread to process things thereafter.
        client.connect(res -> {
            if (!res.succeeded()) {
                logger.error("Couldn't connect to AMQP server.");
                return;
            }

            logger.info("Connected !!");
            AmqpConnection connection = res.result();

            // Set up a producer using the bridge, send a message with it.

            connection.createDynamicReceiver(replyReceiver -> {
                // We got a receiver, the address is provided by the broker
                String replyToAddress = replyReceiver.result().address();
                logger.info("replyToAddress: ", replyToAddress);

                // Attach the handler receiving the reply
                replyReceiver.result().handler(msg -> {
                    logger.info("Got the reply! " + msg.bodyAsString());
                    responseHandler.handle(new AmqpProxyResponse(msg));
                    connection.close(done -> {});
                });

                // Create a sender and send the message:
                connection.createSender(configuration.getQueue(), sender -> {
                    sender.result().send(AmqpMessage.create()
                            .replyTo(replyToAddress)
                            .id("my-message-id")
                            .withBody("This is my request").build());
                });
            });

//            MessageProducer<JsonObject> producer = amqpBridge.createProducer(configuration.getQueue());
//
//            JsonObject amqpMsgPayload = new JsonObject();
//            JsonObject properties = new JsonObject();
//            properties.put("reply-to", "amq.rabbitmq.reply-to");
//            properties.put("correlation-id", corrId);
//            amqpMsgPayload.put("body", "myStringContent");
//            amqpMsgPayload.put("properties", properties);
//
//            logger.info("Sending message to queue: {}", configuration.getQueue());
//            try {
//                producer.send(amqpMsgPayload, response -> {
//                    logger.info("Received {}", response.result().body());
//                    try {
//                        responseHandler.handle(new AmqpProxyResponse(response));
//                    } catch (Exception e) {
//                        logger.error("Response handler error", e);
//                    }
//                });
//            } catch (Exception e) {
//                logger.error("Producer send error", e);
//            }
        });


    }

    @Override
    public ProxyConnection responseHandler(Handler<ProxyResponse> responseHandler) {
        this.responseHandler = responseHandler;
        return this;
    }
}
