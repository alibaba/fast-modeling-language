/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.api.client.dto.property;

/**
 * string property
 *
 * @author panguanjing
 * @date 2022/6/7
 */
public class StringProperty extends BaseClientProperty<String> {
    @Override
    public String valueString() {
        return this.getValue();
    }

    @Override
    public void setValueString(String value) {
        this.setValue(value);
    }

}
