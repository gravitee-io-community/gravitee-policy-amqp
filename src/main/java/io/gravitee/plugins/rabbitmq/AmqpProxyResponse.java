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

import io.gravitee.common.http.HttpHeaders;
import io.gravitee.common.http.MediaType;
import io.gravitee.gateway.api.buffer.Buffer;
import io.gravitee.gateway.api.handler.Handler;
import io.gravitee.gateway.api.proxy.ProxyResponse;
import io.gravitee.gateway.api.stream.ReadStream;
import io.vertx.core.AsyncResult;
import io.vertx.core.eventbus.Message;

public class AmqpProxyResponse implements ProxyResponse {

    private Handler<Buffer> bodyHandler;
    private Handler<Void> endHandler;

    private AsyncResult<Message<Object>> asyncResult;

    AmqpProxyResponse(AsyncResult<Message<Object>> asyncResult) {
        this.asyncResult = asyncResult;
    }

    @Override
    public int status() {
        if (asyncResult.succeeded()) {
            return 200;
        } else if (asyncResult.failed()) {
            return 500;
        }

        return 500;
    }

    @Override
    public HttpHeaders headers() {
        HttpHeaders headers = new HttpHeaders();
        headers.add(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON);
        return headers;
    }

    @Override
    public ReadStream<Buffer> bodyHandler(Handler<Buffer> bodyHandler) {
        this.bodyHandler = bodyHandler;
        return this;
    }

    @Override
    public ReadStream<Buffer> endHandler(Handler<Void> endHandler) {
        this.endHandler = endHandler;
        return this;
    }

    @Override
    public ReadStream<Buffer> resume() {
        if (asyncResult.succeeded()
                && asyncResult.result() != null
                && asyncResult.result().body() != null) {
            this.bodyHandler.handle(Buffer.buffer(asyncResult.result().body().toString()));
        }

        endHandler.handle(null);
        return this;
    }
}
