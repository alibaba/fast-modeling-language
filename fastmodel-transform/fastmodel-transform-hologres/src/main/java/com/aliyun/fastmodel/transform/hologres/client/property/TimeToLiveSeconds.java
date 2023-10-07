/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.hologres.client.property;

import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import org.apache.commons.lang3.StringUtils;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2022/6/16
 */
public class TimeToLiveSeconds extends BaseClientProperty<Long> {

    public static final String TIME_TO_LIVE_IN_SECONDS = "time_to_live_in_seconds";

    public TimeToLiveSeconds() {
        this.setKey(TIME_TO_LIVE_IN_SECONDS);
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
}
