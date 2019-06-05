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
package io.gravitee.plugins.amqp.connection;

import io.gravitee.plugins.amqp.AmqpPolicyConfiguration;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.amqp.AmqpClient;
import io.vertx.amqp.AmqpClientOptions;
import io.vertx.amqp.AmqpConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class AmqpConnectionManager {
    private static final Logger logger = LoggerFactory.getLogger(AmqpConnectionManager.class);

    private Map<AmqpPolicyConfiguration, AmqpClient> clients = new HashMap<>();
    private Map<AmqpPolicyConfiguration, AmqpConnection> connections = new HashMap<>();

    private static final AmqpConnectionManager instance = new AmqpConnectionManager();

    private AmqpConnectionManager() {
        logger.info("AmqpConnectionManager instance created");
    }

    public static AmqpConnectionManager getInstance() {
        return instance;
    }

    public void addConfiguration(Vertx vertx, AmqpPolicyConfiguration configuration) {
        AmqpClientOptions options = new AmqpClientOptions()
                .setHost(configuration.getAmqpServerHostname())
                .setPort(configuration.getAmqpServerPort())
                .setUsername(configuration.getAmqpServerUsername())
                .setPassword(configuration.getAmqpServerPassword());

        clients.put(configuration, AmqpClient.create(vertx, options));
    }

    public void getConnection(AmqpPolicyConfiguration configuration, Handler<AsyncResult<AmqpConnection>> connectionHandler) {
        if (connections.containsKey(configuration)) {
            connectionHandler.handle(Future.succeededFuture(connections.get(configuration)));
            return;
        }

        if (!clients.containsKey(configuration)) {
            logger.error("Unable to connect - no clients for this configuration");
            connectionHandler.handle(Future.failedFuture("Unable to connect - no clients for this configuration"));
            return;
        }

        AmqpClient client = clients.get(configuration);

        logger.info("Connecting to amqp://{}:{}@{}:{}...",
                configuration.getAmqpServerUsername(),
                configuration.getAmqpServerPassword(),
                configuration.getAmqpServerHostname(),
                configuration.getAmqpServerPort());

        client.connect(res -> {
            if (!res.succeeded()) {
                logger.error("Unable to connect");
                connectionHandler.handle(Future.failedFuture("Unable to connect"));
                return;
            }

            AmqpConnection connection = res.result();
            connections.put(configuration, connection);
            connectionHandler.handle(Future.succeededFuture(connection));
        });
    }

    public void closeConnection(AmqpPolicyConfiguration configuration) {
        if (connections.containsKey(configuration)) {
            connections.get(configuration).close(done -> {
                logger.info("Connection closed");
            });
            connections.remove(configuration);
        }
    }
}
