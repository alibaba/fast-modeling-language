/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.api.dialect;

import org.apache.commons.lang3.StringUtils;

/**
 * version
 *
 * @author panguanjing
 * @date 2022/6/16
 */
public interface IVersion {
    IVersion DEFAULT_VERSION = () -> StringUtils.EMPTY;

    /**
     * get version name
     *
     * @return
     */
    String getName();

    /**
     * 获取default
     *
     * @return
     */
    static IVersion getDefault() {
        return DEFAULT_VERSION;
    }
}
