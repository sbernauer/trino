/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.openpolicyagent;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HttpClientUtils
{
    private HttpClientUtils() {}

    public static class RequestSubscriber
            implements Flow.Subscriber<ByteBuffer>
    {
        private final StringBuilder content = new StringBuilder();
        private final CountDownLatch countdown = new CountDownLatch(1);

        @Override
        public void onSubscribe(Flow.Subscription subscription)
        {
            subscription.request(Long.MAX_VALUE);
        }

        @Override
        public void onNext(ByteBuffer buf)
        {
            content.append(StandardCharsets.UTF_8.decode(buf));
        }

        @Override
        public void onError(Throwable throwable)
        {
            this.countdown.countDown();
        }

        @Override
        public void onComplete()
        {
            this.countdown.countDown();
        }

        public String getContent()
        {
            try {
                if (!this.countdown.await(15, TimeUnit.SECONDS)) {
                    throw new RuntimeException("Did not receive request body within expected timeframe");
                }
            }
            catch (InterruptedException e) {
                throw new RuntimeException("Subscriber was interrupted");
            }
            return this.content.toString();
        }
    }

    public static class InstrumentedHttpClient
    {
        private HttpClient mockClient;
        private final List<String> receivedRequests;
        private Function<String, HttpResponse<String>> handler;

        public InstrumentedHttpClient()
                throws InterruptedException, IOException
        {
            this.mockClient = mock(HttpClient.class);
            this.receivedRequests = new LinkedList<>();
            doAnswer(invocation -> {
                RequestSubscriber sub = new RequestSubscriber();
                synchronized (this.receivedRequests) {
                    invocation.getArgument(0, HttpRequest.class).bodyPublisher().get().subscribe(sub);
                    String requestContent = sub.getContent();
                    this.receivedRequests.add(requestContent);
                    if (this.handler != null) {
                        return this.handler.apply(requestContent);
                    }
                    return null;
                }
            }).when(mockClient).send(
                    any(HttpRequest.class),
                    any(HttpResponse.BodyHandler.class));
        }

        public HttpClient getHttpClient()
        {
            return this.mockClient;
        }

        public void setHandler(Function<String, HttpResponse<String>> handler)
        {
            this.handler = handler;
        }

        public List<String> getRequests()
        {
            synchronized (this.receivedRequests) {
                return List.copyOf(this.receivedRequests);
            }
        }

        public void resetRequests()
        {
            this.receivedRequests.clear();
        }
    }

    public static HttpResponse<String> buildResponse(String response, int code)
    {
        HttpResponse<String> mockResponse = mock(HttpResponse.class);
        when(mockResponse.body()).thenReturn(response);
        when(mockResponse.statusCode()).thenReturn(code);
        return mockResponse;
    }
}
