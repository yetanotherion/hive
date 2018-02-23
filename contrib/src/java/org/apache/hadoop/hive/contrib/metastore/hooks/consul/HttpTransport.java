/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.contrib.metastore.hooks.consul;

import org.apache.hadoop.hive.metastore.HiveMetaException;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.DefaultHttpClient;

import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.nio.charset.Charset;

/**
 * HTTP client.  This class is thread safe.
 */
public class HttpTransport {

    static final int DEFAULT_MAX_CONNECTIONS = 1000;
    static final int DEFAULT_MAX_PER_ROUTE_CONNECTIONS = 500;
    static final int DEFAULT_CONNECTION_TIMEOUT = 10000; // 10 sec

    // 10 minutes for read timeout due to blocking queries timeout
    // https://www.consul.io/api/index.html#blocking-queries
    static final int DEFAULT_READ_TIMEOUT = 60000 * 10; // 10 min

    private final HttpClient httpClient;

    public HttpTransport() {
        this.httpClient = new DefaultHttpClient();
    }

    public HttpTransport(HttpClient httpClient) {
        this.httpClient = httpClient;
    }


    public RawResponse makeGetRequest(String url) throws HiveMetaException {
        HttpGet httpGet = new HttpGet(url);
        return executeRequest(httpGet);
    }

    private RawResponse executeRequest(HttpUriRequest httpRequest) throws HiveMetaException {
        try {
            return httpClient.execute(httpRequest, new ResponseHandler<RawResponse>() {
                @Override
                public RawResponse handleResponse(HttpResponse response) throws IOException {
                    int statusCode = response.getStatusLine().getStatusCode();
                    String statusMessage = response.getStatusLine().getReasonPhrase();

                    String content = EntityUtils.toString(response.getEntity(), Charset.forName("UTF-8"));

                    Long consulIndex = parseUnsignedLong(response.getFirstHeader("X-Consul-Index"));
                    Boolean consulKnownLeader = parseBoolean(response.getFirstHeader("X-Consul-Knownleader"));
                    Long consulLastContact = parseUnsignedLong(response.getFirstHeader("X-Consul-Lastcontact"));

                    return new RawResponse(statusCode, statusMessage, content, consulIndex, consulKnownLeader, consulLastContact);
                }
            });
        } catch (IOException e) {
            throw new HiveMetaException(e);
        }
    }

    private Long parseUnsignedLong(Header header) {
        if (header == null) {
            return null;
        }

        String value = header.getValue();
        if (value == null) {
            return null;
        }

        try {
            return parseUnsignedLong(value);
        } catch (Exception e) {
            return null;
        }
    }

    public static long parseUnsignedLong(String s) {
        if (s.charAt(0) == '-') {
            throw new NumberFormatException("An unsigned long was expected. Cannot parse negative number " + s);
        }
        int length = s.length();
        // Long.MAX_VALUE is 19 digits in length so anything
        // shorter than that is trivial to parse.
        if (length < 19) {
            return Long.parseLong(s);
        }
        long front = Long.parseLong(s.substring(0, length - 1));
        int onesDigit = Character.digit(s.charAt(length - 1), 10);
        if (onesDigit < 0) {
            throw new NumberFormatException("Invalid last digit for " + onesDigit);
        }
        long result = front * 10 + onesDigit;
        if (compareLong(result + Long.MIN_VALUE, front + Long.MIN_VALUE) < 0) {
            throw new NumberFormatException("The number " + s + " is greater than 2^64");
        }
        return result;
    }

    private static int compareLong(long x, long y) {
        return (x < y) ? -1 : ((x == y) ? 0 : 1);
    }

    private Boolean parseBoolean(Header header) {
        if (header == null) {
            return null;
        }

        if ("true".equals(header.getValue())) {
            return true;
        }

        if ("false".equals(header.getValue())) {
            return false;
        }

        return null;
    }
}
