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

/**
 * Represents a consul raw response
 */
public final class RawResponse {

    private final int statusCode;
    private final String statusMessage;

    private final String content;

    private final Long consulIndex;
    private final Boolean consulKnownLeader;
    private final Long consulLastContact;

    public RawResponse(int statusCode, String statusMessage, String content, Long consulIndex, Boolean consulKnownLeader, Long consulLastContact) {
        this.statusCode = statusCode;
        this.statusMessage = statusMessage;
        this.content = content;
        this.consulIndex = consulIndex;
        this.consulKnownLeader = consulKnownLeader;
        this.consulLastContact = consulLastContact;
    }

    public int getStatusCode() {
        return statusCode;
    }

    public String getStatusMessage() {
        return statusMessage;
    }

    public String getContent() {
        return content;
    }

    public Long getConsulIndex() {
        return consulIndex;
    }

    public Boolean isConsulKnownLeader() {
        return consulKnownLeader;
    }

    public Long getConsulLastContact() {
        return consulLastContact;
    }
}
