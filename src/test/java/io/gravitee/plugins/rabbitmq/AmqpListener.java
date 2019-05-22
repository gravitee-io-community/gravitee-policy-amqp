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

import io.vertx.core.Vertx;
import io.vertx.ext.amqp.*;

import java.util.UUID;

public class AmqpListener {
    public static void main(String[] args) throws Exception {

        AmqpClientOptions options = new AmqpClientOptions()
                .setHost("localhost")
                .setPort(5672)
                .setUsername("guest")
                .setPassword("guest");

        AmqpClient client = AmqpClient.create(Vertx.vertx(), options);

// SERVer
        client.createReceiver("random",
                msg -> {
                    // called on every received messages
                    System.out.println("random Received " + msg.bodyAsString() + " Id " + msg.id());

                    client.createSender(msg.replyTo(), done -> {
                        if (done.failed()) {
                            System.out.println("Unable to create a sender");
                            return;
                        }
                        AmqpSender sender = done.result();

                        try {
                            Thread.sleep(500);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }

                        sender.sendWithAck(AmqpMessage.create().withBody("hello from random").id(msg.id()).build(), acked -> {
                            if (acked.succeeded()) {
                                System.out.println("Reply Message accepted");
                            } else {
                                System.out.println("Message not accepted");
                            }
                        });
                    });
                },
                done -> {
                    if (done.failed()) {
                        System.out.println("Unable to create receiver");
                    } else {
                        AmqpReceiver receiver = done.result();
                    }
                }
        );

// END SERVer

        client.connect(ar -> {
            if (ar.failed()) {
                System.out.println("Unable to connect to the broker");
                return;
            }
            System.out.println("Connection succeeded");
            AmqpConnection connection = ar.result();

            String corId = UUID.randomUUID().toString();
            System.out.println("CorID: " + corId);

// CLIENT
            connection.createSender("random", done -> {
                if (done.failed()) {
                    System.out.println("Unable to create a sender");
                    return;
                }
                AmqpSender sender = done.result();
                System.out.println("Sender created");

                //sender.sendWithAck(AmqpMessage.create().withBody("hello").replyTo("amq.rabbitmq.reply-to").id(corId).build(), acked -> {
                try {
                    sender.sendWithAck(AmqpMessage.create().withBody("hello").id(corId).replyTo("random-reply").build(), acked -> {
                        if (!acked.succeeded()) {
                            System.out.println("Message not accepted");
                            return;
                        }
                        System.out.println("Message accepted");

                        connection.createReceiver("random-reply",
                                msg -> {
                                    // called on every received messages
                                    System.out.println("random-reply: Received " + msg.bodyAsString() + " Id " + msg.id());
                                },
                                done2 -> {
                                    if (done2.failed()) {
                                        System.out.println("Unable to create receiver");
                                    } else {
                                        System.out.println("Created receiver");
                                    }
                                }
                        );

                    });
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });

//END CLIENT

        });


    }
}
