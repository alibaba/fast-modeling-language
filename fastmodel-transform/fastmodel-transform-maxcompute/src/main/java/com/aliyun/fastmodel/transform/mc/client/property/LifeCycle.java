/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.mc.client.property;

import java.util.concurrent.TimeUnit;

import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import org.apache.commons.lang3.StringUtils;

/**
 * LifeCycle
 *
 * @author panguanjing
 * @date 2022/6/16
 */
public class LifeCycle extends BaseClientProperty<Long> {

    public static final String LIFECYCLE = "life_cycle";

    public LifeCycle() {
        this.setKey(LIFECYCLE);
    }

    @Override
    public String valueString() {
        return String.valueOf(this.getValue());
    }

    @Override
    public void setValueString(String value) {
        if (StringUtils.isBlank(value)) {
            this.setValue(null);
        }
        this.setValue(Long.parseLong(value));
    }

    /**
     * to seconds
     *
     * @return seconds
     */
    public Long toSeconds() {
        return TimeUnit.DAYS.toSeconds(this.getValue());
    }

    /**
     * to Lifecycle
     *
     * @param seconds
     * @return
     */
    public static Long toLifeCycle(Long seconds) {
        long convert = TimeUnit.DAYS.convert(seconds, TimeUnit.SECONDS);
        return convert == 0 ? 1L : convert;
    }
}
