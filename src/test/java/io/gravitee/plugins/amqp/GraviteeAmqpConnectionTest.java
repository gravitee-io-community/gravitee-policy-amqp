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

import io.gravitee.common.util.ServiceLoaderHelper;
import io.gravitee.gateway.api.ExecutionContext;
import io.gravitee.gateway.api.buffer.Buffer;
import io.gravitee.gateway.api.buffer.BufferFactory;
import io.gravitee.gateway.api.proxy.ProxyResponse;
import io.gravitee.plugins.amqp.connection.AmqpConnectionManager;
import io.gravitee.plugins.amqp.response.AmqpProxyResponse;
import io.gravitee.plugins.amqp.response.FailedAmqpProxyResponse;
import io.vertx.amqp.AmqpConnection;
import io.vertx.amqp.AmqpMessage;
import io.vertx.amqp.AmqpSender;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.internal.stubbing.answers.AnswersWithDelay;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.*;

import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class GraviteeAmqpConnectionTest {

    @Mock
    AmqpConnectionManager connectionManager;

    @Mock
    private ExecutionContext mockExecutionContext;

    @Mock
    private AmqpPolicyConfiguration mockConfiguration;

    @Mock
    AmqpConnection connection;

    AmqpSender sender;

    GraviteeAmqpConnection proxtService;

    @Mock
    io.gravitee.gateway.api.handler.Handler<ProxyResponse> responseHandler;
    private BufferFactory factory;

    private final String qName = "random";
    private final String replyQName = "random-reply";
    private String currentCorrelationId;

    @Before
    public void setUp() throws Exception {
        factory = ServiceLoaderHelper.loadFactory(BufferFactory.class);
        sender = Mockito.mock(AmqpSender.class, withSettings().withoutAnnotations());

        when(mockConfiguration.getQueue()).thenReturn(qName);
        when(mockConfiguration.isRequestResponse()).thenReturn(false);

        proxtService = new GraviteeAmqpConnection(mockExecutionContext, mockConfiguration, connectionManager);
        proxtService.responseHandler(responseHandler);
    }

    @Test
    public void whenNoConnectionToAMQP_serviceReturnsFailedResponse() throws Exception {
        givenAMQPFailedToConnect();

        proxtService.end();

        thenProxyReturnsFailedResponse();
    }

    @Test
    public void whenConnectionCannotCreateSender_serviceReturnsFailedResponse() throws Exception {
        givenAMQPConnected();
        givenFailedSender();

        proxtService.end();

        thenProxyReturnsFailedResponse();
    }

    @Test
    public void whenMessagePosted_serviceReturnsSuccessfulResponse() throws Exception {
        String incomingMessage = "{\"point\":1}";

        givenAMQPConnected();
        givenSender();

        givenSuccessfulMessageSent(incomingMessage);

        Buffer buffer = factory.buffer(incomingMessage);
        proxtService.write(buffer);
        proxtService.end();

        thenProxyReturnsSuccessfulResponse();
    }

    @Ignore
    @Test
    public void whenMessagePostedInRequestReply_serviceReturnsSuccessfulResponse() throws Exception {
        String incomingMessage = "{\"point\":1}";
        String responseMessage = "{\"response\":2}";
        when(mockConfiguration.isRequestResponse()).thenReturn(true);

        givenAMQPConnected();
        givenSender();
        givenReceiver(responseMessage);

        givenSuccessfulMessageSent(incomingMessage);

        Buffer buffer = factory.buffer(incomingMessage);
        proxtService.write(buffer);
        proxtService.end();

        thenProxyReturnsSuccessfulResponse();
    }

    private void givenAMQPFailedToConnect() {
        doAnswer(invocation -> {
            Handler<AsyncResult<AmqpConnection>> connectionHandler = invocation.getArgument(1);
            connectionHandler.handle(getFailedConnectionResult());
            return null;
        }).when(connectionManager).getConnection(any(), any());
    }

    private void givenAMQPConnected() {
        doAnswer(invocation -> {
            Handler<AsyncResult<AmqpConnection>> connectionHandler = invocation.getArgument(1);
            connectionHandler.handle(getSuccessfulConnection());
            return null;
        }).when(connectionManager).getConnection(any(), any());
    }

    private void givenFailedSender() {
        doAnswer(invocation -> {
            Handler<AsyncResult<AmqpConnection>> connectionHandler = invocation.getArgument(1);
            connectionHandler.handle(getFailedSender());
            return null;
        }).when(connection).createSender(eq(qName), any());
    }

    private void givenSender() {
        doAnswer(invocation -> {
            Handler<AsyncResult<AmqpConnection>> connectionHandler = invocation.getArgument(1);
            connectionHandler.handle(getSuccessfulSender());
            return null;
        }).when(connection).createSender(eq(qName), any());
    }

    private void givenReceiver(String responseMessage) {
        doAnswer(new AnswersWithDelay(500, invocation -> {
            Handler<AmqpMessage> msg = invocation.getArgument(1);
            msg.handle(getMockedResponseMsg(responseMessage));
            return null;
        })).when(connection).createReceiver(eq(replyQName), any(Handler.class), any());
    }

    private AmqpMessage getMockedResponseMsg(String responseMessage) {
        AmqpMessage msg = Mockito.mock(AmqpMessage.class);
        when(msg.bodyAsString()).thenReturn(responseMessage);
        when(msg.correlationId()).then(invocation -> {
            return currentCorrelationId;
        });
        return msg;
    }

    private AsyncResult getFailedConnectionResult() {
        AsyncResult result = mock(AsyncResult.class);
        when(result.failed()).thenReturn(true);
        return result;
    }

    private AsyncResult getSuccessfulConnection() {
        AsyncResult result = mock(AsyncResult.class);
        when(result.failed()).thenReturn(false);
        when(result.result()).thenReturn(connection);
        return result;
    }

    private AsyncResult getFailedSender() {
        AsyncResult result = mock(AsyncResult.class);
        when(result.failed()).thenReturn(true);
        return result;
    }

    private AsyncResult getSuccessfulSender() {
        AsyncResult result = mock(AsyncResult.class);
        when(result.failed()).thenReturn(false);
        when(result.result()).thenReturn(sender);
        return result;
    }

    private AsyncResult getSuccessfulReceiver() {
        AsyncResult result = mock(AsyncResult.class);
        when(result.failed()).thenReturn(false);
        when(result.result()).thenReturn(sender);
        return result;
    }

    private AsyncResult getSuccessfulAck() {
        AsyncResult result = mock(AsyncResult.class);
        when(result.failed()).thenReturn(false);
        return result;
    }

    private void givenSuccessfulMessageSent(String incomingMessage) {
        doAnswer(invocation -> {
            AmqpMessage msg = invocation.getArgument(0);

            thenOutgoingMessageIsCorrect(incomingMessage, msg);
            currentCorrelationId = msg.correlationId();

            Handler<AsyncResult<AmqpConnection>> connectionHandler = invocation.getArgument(1);
            connectionHandler.handle(getSuccessfulAck());
            return null;
        }).when(sender).sendWithAck(any(), any());
    }

    private void thenOutgoingMessageIsCorrect(String incomingMessage, AmqpMessage msg) {
        assertEquals(incomingMessage, msg.bodyAsString());
        assertEquals(replyQName, msg.replyTo());
        assertNotNull(msg.correlationId());
    }

    private void thenProxyReturnsFailedResponse() {
        verify(responseHandler, times(1)).handle(Mockito.any(FailedAmqpProxyResponse.class));
    }

    private void thenProxyReturnsSuccessfulResponse() {
        verify(responseHandler, times(1)).handle(Mockito.any(AmqpProxyResponse.class));
    }

    private void thenProxyReturnsSuccessfulResponse(String responseMessage) {
        verify(responseHandler, times(1)).handle(Mockito.any(AmqpProxyResponse.class));
    }

}