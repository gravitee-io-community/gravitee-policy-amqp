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

import io.gravitee.policy.api.PolicyConfiguration;

@SuppressWarnings("unused")
public class AmqpPolicyConfiguration implements PolicyConfiguration {

    private String queue = "gravitee-api";

    private String amqpServerHostname = "localhost";

    private int amqpServerPort = 5762;

    private String amqpServerUsername = "guest";

    private String amqpServerPassword = "guest";

    private boolean requestResponse = false;

    public void setQueue(String queue) {
        this.queue = queue;
    }

    public void setAmqpServerHostname(String amqpServerHostname) {
        this.amqpServerHostname = amqpServerHostname;
    }

    public void setAmqpServerPort(int amqpServerPort) {
        this.amqpServerPort = amqpServerPort;
    }

    public void setAmqpServerPassword(String amqpServerPassword) {
        this.amqpServerPassword = amqpServerPassword;
    }

    public void setAmqpServerUsername(String amqpServerUsername) {
        this.amqpServerUsername = amqpServerUsername;
    }

    public String getQueue() {
        return queue;
    }

    public int getAmqpServerPort() {
        return amqpServerPort;
    }

    public String getAmqpServerHostname() {
        return amqpServerHostname;
    }

    public String getAmqpServerPassword() {
        return amqpServerPassword;
    }

    public String getAmqpServerUsername() {
        return amqpServerUsername;
    }

    public boolean isRequestResponse() {
        return requestResponse;
    }

    public void setRequestResponse(boolean requestResponse) {
        this.requestResponse = requestResponse;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AmqpPolicyConfiguration that = (AmqpPolicyConfiguration) o;

        if (amqpServerPort != that.amqpServerPort) return false;
        if (!amqpServerHostname.equals(that.amqpServerHostname)) return false;
        if (amqpServerUsername != null ? !amqpServerUsername.equals(that.amqpServerUsername) : that.amqpServerUsername != null)
            return false;
        return amqpServerPassword != null ? amqpServerPassword.equals(that.amqpServerPassword) : that.amqpServerPassword == null;
    }

    @Override
    public int hashCode() {
        int result = amqpServerHostname.hashCode();
        result = 31 * result + amqpServerPort;
        result = 31 * result + (amqpServerUsername != null ? amqpServerUsername.hashCode() : 0);
        result = 31 * result + (amqpServerPassword != null ? amqpServerPassword.hashCode() : 0);
        return result;
    }
}
