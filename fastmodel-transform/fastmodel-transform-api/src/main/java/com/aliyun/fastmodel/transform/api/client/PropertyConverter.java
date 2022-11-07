/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.api.client;

import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;

/**
 * PropertyConverter
 *
 * @author panguanjing
 * @date 2022/6/29
 */
public interface PropertyConverter {
    /**
     * 根据name和value获取baseClientProperty
     *
     * @param name
     * @param value
     * @return
     */
    default BaseClientProperty create(String name, String value) {
        throw new UnsupportedOperationException();
    }
}
