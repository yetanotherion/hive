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

import com.google.gson.annotations.SerializedName;

import java.util.List;
import java.util.Map;

/**
 * Represents a consul health check
 */
public class HealthService {

    public static class Node {

        @SerializedName("ID")
        private String id;

        @SerializedName("Node")
        private String node;

        @SerializedName("Address")
        private String address;

        @SerializedName("Datacenter")
        private String datacenter;

        @SerializedName("TaggedAddresses")
        private Map<String, String> taggedAddresses;

        @SerializedName("Meta")
        private Map<String, String> meta;

        @SerializedName("CreateIndex")
        private Long createIndex;

        @SerializedName("ModifyIndex")
        private Long modifyIndex;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getNode() {
            return node;
        }

        public void setNode(String node) {
            this.node = node;
        }

        public String getAddress() {
            return address;
        }

        public void setAddress(String address) {
            this.address = address;
        }

        public String getDatacenter() {
            return datacenter;
        }

        public void setDatacenter(String datacenter) {
            this.datacenter = datacenter;
        }

        public Map<String, String> getTaggedAddresses() {
            return taggedAddresses;
        }

        public void setTaggedAddresses(Map<String, String> taggedAddresses) {
            this.taggedAddresses = taggedAddresses;
        }

        public Map<String, String> getMeta() {
            return meta;
        }

        public void setMeta(Map<String, String> meta) {
            this.meta = meta;
        }

        public Long getCreateIndex() {
            return createIndex;
        }

        public void setCreateIndex(Long createIndex) {
            this.createIndex = createIndex;
        }

        public Long getModifyIndex() {
            return modifyIndex;
        }

        public void setModifyIndex(Long modifyIndex) {
            this.modifyIndex = modifyIndex;
        }

        @Override
        public String toString() {
            return "Node{" +
                    "id='" + id + '\'' +
                    ", node='" + node + '\'' +
                    ", address='" + address + '\'' +
                    ", datacenter='" + datacenter + '\'' +
                    ", taggedAddresses=" + taggedAddresses +
                    ", meta=" + meta +
                    ", createIndex=" + createIndex +
                    ", modifyIndex=" + modifyIndex +
                    '}';
        }
    }

    public static class Service {
        @SerializedName("ID")
        private String id;

        @SerializedName("Service")
        private String service;

        @SerializedName("Tags")
        private List<String> tags;

        @SerializedName("Address")
        private String address;

        @SerializedName("Port")
        private Integer port;

        @SerializedName("EnableTagOverride")
        private Boolean enableTagOverride;

        @SerializedName("CreateIndex")
        private Long createIndex;

        @SerializedName("ModifyIndex")
        private Long modifyIndex;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getService() {
            return service;
        }

        public void setService(String service) {
            this.service = service;
        }

        public List<String> getTags() {
            return tags;
        }

        public void setTags(List<String> tags) {
            this.tags = tags;
        }

        public String getAddress() {
            return address;
        }

        public void setAddress(String address) {
            this.address = address;
        }

        public Integer getPort() {
            return port;
        }

        public void setPort(Integer port) {
            this.port = port;
        }

        public Boolean getEnableTagOverride() {
            return enableTagOverride;
        }

        public void setEnableTagOverride(Boolean enableTagOverride) {
            this.enableTagOverride = enableTagOverride;
        }

        public Long getCreateIndex() {
            return createIndex;
        }

        public void setCreateIndex(Long createIndex) {
            this.createIndex = createIndex;
        }

        public Long getModifyIndex() {
            return modifyIndex;
        }

        public void setModifyIndex(Long modifyIndex) {
            this.modifyIndex = modifyIndex;
        }

        @Override
        public String toString() {
            return "Service{" +
                    "id='" + id + '\'' +
                    ", service='" + service + '\'' +
                    ", tags=" + tags +
                    ", address='" + address + '\'' +
                    ", port=" + port +
                    ", enableTagOverride=" + enableTagOverride +
                    ", createIndex=" + createIndex +
                    ", modifyIndex=" + modifyIndex +
                    '}';
        }
    }

    @SerializedName("Node")
    private Node node;

    @SerializedName("Service")
    private Service service;

    @SerializedName("Checks")
    private List<Check> checks;

    public Node getNode() {
        return node;
    }

    public void setNode(Node node) {
        this.node = node;
    }

    public Service getService() {
        return service;
    }

    public void setService(Service service) {
        this.service = service;
    }

    public List<Check> getChecks() {
        return checks;
    }

    public void setChecks(List<Check> checks) {
        this.checks = checks;
    }

    @Override
    public String toString() {
        return "HealthService{" +
                "node=" + node +
                ", service=" + service +
                ", checks=" + checks +
                '}';
    }
}
