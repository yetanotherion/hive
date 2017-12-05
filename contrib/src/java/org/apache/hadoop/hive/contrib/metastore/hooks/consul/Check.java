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

/**
 * Represents a consul check.
 */
public class Check {

	public static enum CheckStatus {
		@SerializedName("unknown")
		UNKNOWN,
		@SerializedName("passing")
		PASSING,
		@SerializedName("warning")
		WARNING,
		@SerializedName("critical")
		CRITICAL
	}

	@SerializedName("Node")
	private String node;

	@SerializedName("CheckID")
	private String checkId;

	@SerializedName("Name")
	private String name;

	@SerializedName("Status")
	private CheckStatus status;

	@SerializedName("Notes")
	private String notes;

	@SerializedName("Output")
	private String output;

	@SerializedName("ServiceID")
	private String serviceId;

	@SerializedName("ServiceName")
	private String serviceName;

	@SerializedName("ServiceTags")
	private List<String> serviceTags;

	@SerializedName("CreateIndex")
	private Long createIndex;

	@SerializedName("ModifyIndex")
	private Long modifyIndex;

	public String getNode() {
		return node;
	}

	public void setNode(String node) {
		this.node = node;
	}

	public String getCheckId() {
		return checkId;
	}

	public void setCheckId(String checkId) {
		this.checkId = checkId;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public CheckStatus getStatus() {
		return status;
	}

	public void setStatus(CheckStatus status) {
		this.status = status;
	}

	public String getNotes() {
		return notes;
	}

	public void setNotes(String notes) {
		this.notes = notes;
	}

	public String getOutput() {
		return output;
	}

	public void setOutput(String output) {
		this.output = output;
	}

	public String getServiceId() {
		return serviceId;
	}

	public void setServiceId(String serviceId) {
		this.serviceId = serviceId;
	}

	public String getServiceName() {
		return serviceName;
	}

	public void setServiceName(String serviceName) {
		this.serviceName = serviceName;
	}

	public List<String> getServiceTags() {
		return serviceTags;
	}

	public void setServiceTags(List<String> serviceTags) {
		this.serviceTags = serviceTags;
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
		return "Check{" +
				"node='" + node + '\'' +
				", checkId='" + checkId + '\'' +
				", name='" + name + '\'' +
				", status=" + status +
				", notes='" + notes + '\'' +
				", output='" + output + '\'' +
				", serviceId='" + serviceId + '\'' +
				", serviceName='" + serviceName + '\'' +
				", serviceTags=" + serviceTags +
				", createIndex=" + createIndex +
				", modifyIndex=" + modifyIndex +
				'}';
	}
}
