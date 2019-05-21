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
import io.vertx.amqpbridge.AmqpBridge;
import io.vertx.amqpbridge.AmqpBridgeOptions;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.MessageProducer;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AmqpConnection implements ProxyConnection {

    private static final Logger logger = LoggerFactory.getLogger(AmqpConnection.class);

    private AmqpBridge amqpBridge;

    private Buffer content;

    private Handler<ProxyResponse> responseHandler;

    private AmqpPolicyConfiguration configuration;

    AmqpConnection(ExecutionContext executionContext, AmqpPolicyConfiguration configuration) {
        this.configuration = configuration;
        Vertx vertx = executionContext.getComponent(Vertx.class);

        AmqpBridgeOptions bridgeOptions = new AmqpBridgeOptions();
        this.amqpBridge = AmqpBridge.create(vertx);
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
        amqpBridge.start(configuration.getAmqpServerHostname(),
                         configuration.getAmqpServerPort(),
                         configuration.getAmqpServerUsername(),
                         configuration.getAmqpServerPassword(), res -> {
            if (!res.succeeded()) {
                logger.error("Couldn't connect to AMQP server.");
                return;
            }

            logger.info("Connected !!");

            // Set up a producer using the bridge, send a message with it.
            MessageProducer<JsonObject> producer = amqpBridge.createProducer(configuration.getQueue());
            JsonObject amqpMsgPayload = new JsonObject();
            amqpMsgPayload.put("body", "myStringContent");

            logger.info("Sending message to queue: {}", configuration.getQueue());
            producer.send(amqpMsgPayload, response -> {
                logger.info("Received {}", response.result().body());
                responseHandler.handle(new AmqpProxyResponse(response));
            });
        });


    }

    @Override
    public ProxyConnection responseHandler(Handler<ProxyResponse> responseHandler) {
        this.responseHandler = responseHandler;
        return this;
    }
}
