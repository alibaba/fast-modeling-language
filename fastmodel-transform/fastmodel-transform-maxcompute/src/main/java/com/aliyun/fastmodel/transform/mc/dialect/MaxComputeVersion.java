/*
 * Copyright 2022 Alibaba Group Holding Ltd.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
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
