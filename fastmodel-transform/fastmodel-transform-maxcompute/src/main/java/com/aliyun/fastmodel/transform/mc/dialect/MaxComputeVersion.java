/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.mc.dialect;

import com.aliyun.fastmodel.transform.api.dialect.IVersion;
import lombok.Getter;

/**
 * maxcompute version
 *
 * @author panguanjing
 * @date 2022/6/16
 */
public enum MaxComputeVersion implements IVersion {
    /**
     * 1.0
     */
    V1(Constants.V1),
    /**
     * 2.0
     */
    V2(Constants.V2);

    @Getter
    private final String value;

    MaxComputeVersion(String value) {this.value = value;}

    public static class Constants {
        public static final String V1 = "1.0";
        public static final String V2 = "2.0";
    }

    @Override
    public String getName() {
        return this.getValue();
    }
}
