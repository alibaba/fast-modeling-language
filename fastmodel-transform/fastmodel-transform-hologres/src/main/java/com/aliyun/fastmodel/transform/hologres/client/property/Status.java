package com.aliyun.fastmodel.transform.hologres.client.property;

import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2023/6/27
 */
public enum Status {

    /**
     * on
     */
    ON("on"),
    /**
     * off
     */
    OFF("off"),
    /**
     * auto
     */
    AUTO("auto");

    @Getter
    private final String value;

    Status(String value) {
        this.value = value;
    }

    public static Status getByValue(String value) {
        Status[] statuses = Status.values();
        for (Status status : statuses) {
            if (StringUtils.equalsIgnoreCase(status.getValue(), value)) {
                return status;
            }
        }
        return Status.AUTO;
    }

}
