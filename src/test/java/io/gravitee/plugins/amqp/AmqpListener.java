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
package io.gravitee.plugins.amqp;

import io.vertx.amqp.AmqpClient;
import io.vertx.amqp.AmqpClientOptions;
import io.vertx.amqp.AmqpMessage;
import io.vertx.amqp.AmqpSender;
import io.vertx.core.Vertx;

public class AmqpListener {

    public static void main(String[] args) throws Exception {

        AmqpClientOptions options = new AmqpClientOptions()
                .setHost("localhost")
                .setPort(5672)
                .setUsername("guest")
                .setPassword("guest");

        AmqpClient client = AmqpClient.create(Vertx.vertx(), options);

        client.createReceiver("random",
                msg -> {
                    // called on every received messages
                    System.out.println("random Received " + msg.bodyAsString() + " Id " + msg.id()+ " corID: " + msg.correlationId());

                    client.createSender(msg.replyTo(), done -> {
                        if (done.failed()) {
                            System.out.println("Unable to create a sender");
                            return;
                        }
                        AmqpSender sender = done.result();
                        AmqpMessage replyMessage = AmqpMessage.create()
                                .withBody("hello from random. Echo: " + msg.bodyAsString())
                                .correlationId(msg.correlationId())
                                .build();

                        sender.sendWithAck(replyMessage, acked -> {
                            if (acked.succeeded()) {
                                System.out.println("Reply Message accepted, corId: " + replyMessage.correlationId());
                            } else {
                                System.out.println("Message not accepted");
                            }
                        });
                    });
                },
                done -> {
                    if (done.failed()) {
                        System.out.println("Unable to create receiver");
                        return;
                    }
                    System.out.println("Listening for messages...");
                }
        );
    }
}
