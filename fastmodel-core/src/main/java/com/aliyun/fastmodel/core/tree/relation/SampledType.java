/*
 * Copyright 2021-2022 Alibaba Group Holding Ltd.
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

package com.aliyun.fastmodel.core.tree.relation;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2020/11/10
 */
public enum SampledType {
    /**
     *
     */
    BERNOULLI,
    /**
     *
     */
    SYSTEM;

    /**
     * get by code
     *
     * @param text text
     * @return SampledType
     */
    public static SampledType getByCode(String text) {
        SampledType[] sampledTypes = SampledType.values();
        for (SampledType sampledType : sampledTypes) {
            if (sampledType.name().equalsIgnoreCase(text)) {
                return sampledType;
            }
        }
        throw new IllegalArgumentException("can't find the type with code:" + text);
    }
}
