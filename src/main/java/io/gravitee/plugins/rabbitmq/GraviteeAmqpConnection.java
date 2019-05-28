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
import io.gravitee.plugins.rabbitmq.response.AmqpProxyResponse;
import io.gravitee.plugins.rabbitmq.response.FailedAmqpProxyResponse;
import io.vertx.core.Vertx;
import io.vertx.ext.amqp.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class GraviteeAmqpConnection implements ProxyConnection {
    private static final Logger logger = LoggerFactory.getLogger(GraviteeAmqpConnection.class);
    public static final String DEFAULT_RESPONSE = "{\"result\":true}";

    private AmqpConnectionManager connectionManager;

    private Buffer content;

    private Handler<ProxyResponse> responseHandler;

    private AmqpPolicyConfiguration configuration;

    GraviteeAmqpConnection(ExecutionContext executionContext, AmqpPolicyConfiguration configuration, AmqpConnectionManager connectionManager) {
        this.configuration = configuration;
        this.connectionManager = connectionManager;

        Vertx vertx = executionContext.getComponent(Vertx.class);
//        AmqpClientOptions options = new AmqpClientOptions()
//                .setHost(configuration.getAmqpServerHostname())
//                .setPort(configuration.getAmqpServerPort())
//                .setUsername(configuration.getAmqpServerUsername())
//                .setPassword(configuration.getAmqpServerPassword());
//
//        this.client = AmqpClient.create(vertx, options);

        connectionManager.addConfiguration(vertx, configuration);
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

        connectionManager.getConnection(configuration, res -> {
            if (!res.succeeded()) {
                logger.error("Couldn't connect to AMQP server.");
                responseHandler.handle(new FailedAmqpProxyResponse());
                return;
            }

            logger.info("Connected");
            AmqpConnection connection = res.result();
            String corId = UUID.randomUUID().toString();
            String replyQName = configuration.getQueue().concat("-reply");
            String messageBody = (content != null) ? content.toString() : "";

            if (configuration.isRequestResponse()) {
                connection.createReceiver(replyQName,
                        msg -> {
                            // called on every received messages
                            logger.info("random-reply: Received " + msg.bodyAsString() + " Id " + msg.id());
                            sendSuccessfulResponse(msg.bodyAsString());
                        },
                        done2 -> {
                            if (done2.failed()) {
                                logger.error("Unable to create receiver");
                                sendErrorResponse();
                                return;
                            }
                            logger.info("Created receiver");
                        }
                );
            }

            connection.createSender(configuration.getQueue(), done -> {
                if (done.failed()) {
                    logger.error("Unable to create a sender");
                    sendErrorResponse();
                    return;
                }
                AmqpSender sender = done.result();
                logger.info("Sender created");

                //sender.sendWithAck(AmqpMessage.create().withBody("hello").replyTo("amq.rabbitmq.reply-to").id(corId).build(), acked -> {
                try {
                    sender.sendWithAck(AmqpMessage.create().withBody(messageBody).id(corId).replyTo(replyQName).build(), acked -> {
                        if (!acked.succeeded()) {
                            logger.error("Sent Message not accepted");
                            sendErrorResponse();
                            return;
                        }
                        logger.info("Message accepted");
                        if (!configuration.isRequestResponse()) {
                            sendSuccessfulResponse(DEFAULT_RESPONSE);
                        }
                    });
                } catch (Exception e) {
                    logger.error("Exception posting a message", e);
                    sendErrorResponse();
                }
            });
        });
    }

    private void sendSuccessfulResponse(String response) {
        responseHandler.handle(new AmqpProxyResponse(response));
        connectionManager.closeConnection(configuration);
    }

    private void sendErrorResponse() {
        responseHandler.handle(new FailedAmqpProxyResponse());
        connectionManager.closeConnection(configuration);
//        connection[0].close(done -> {
//            logger.info("Connection closed");
//        });
    }

    @Override
    public ProxyConnection responseHandler(Handler<ProxyResponse> responseHandler) {
        this.responseHandler = responseHandler;
        return this;
    }
}
