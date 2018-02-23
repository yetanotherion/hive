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
 * Represents a consul response
 */
public final class Response<T> {

    private final T value;

    private final Long consulIndex;
    private final Boolean consulKnownLeader;
    private final Long consulLastContact;

    public Response(T value, Long consulIndex, Boolean consulKnownLeader, Long consulLastContact) {
        this.value = value;
        this.consulIndex = consulIndex;
        this.consulKnownLeader = consulKnownLeader;
        this.consulLastContact = consulLastContact;
    }

    public Response(T value, RawResponse rawResponse) {
        this(value, rawResponse.getConsulIndex(), rawResponse.isConsulKnownLeader(), rawResponse.getConsulLastContact());
    }

    public T getValue() {
        return value;
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

    @Override
    public String toString() {
        return "Response{" +
                "value=" + value +
                ", consulIndex=" + consulIndex +
                ", consulKnownLeader=" + consulKnownLeader +
                ", consulLastContact=" + consulLastContact +
                '}';
    }
}
