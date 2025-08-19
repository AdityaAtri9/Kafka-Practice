package com.ncp.kafka;

public class DeviceStatus {
	private String deviceId;
	private String status;
	private String timestamp;
	
	public DeviceStatus()
	{
		
	}
	

	public DeviceStatus(String deviceId, String status, String timestamp)
	{
		this.deviceId = deviceId;
		this.status = status;
		this.timestamp = timestamp;
	}

	public String getDeviceId() {
		return deviceId;
	}

	public void setDeviceId(String deviceId) {
		this.deviceId = deviceId;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public String getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
	}

	@Override
	public String toString() {
		return "DeviceStatus [deviceId=" + deviceId + ", status=" + status + ", timestamp=" + timestamp + "]";
	}
}
