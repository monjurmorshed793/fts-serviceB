package org.ums.faulttolerantsystemserviceb;

import java.io.Serializable;

public class ServiceStatus implements Serializable{
    private String parentServiceId;

    private String serviceId;

    private boolean status;

    public ServiceStatus() {
    }

    public String getParentServiceId() {
        return parentServiceId;
    }

    public void setParentServiceId(String parentServiceId) {
        this.parentServiceId = parentServiceId;
    }

    public String getServiceId() {
        return serviceId;
    }

    public void setServiceId(String serviceId) {
        this.serviceId = serviceId;
    }

    public boolean isStatus() {
        return status;
    }

    public void setStatus(boolean status) {
        this.status = status;
    }
}
