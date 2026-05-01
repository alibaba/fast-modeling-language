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

package com.aliyun.fastmodel.driver.client.command.tenant;

import lombok.Getter;

/**
 * 查询的参数定义的地方
 *
 * @author panguanjing
 * @date 2020/12/14
 */
public enum QueryParams {

    /**
     * 租户的baseKey
     */
    BASE_KEY(TenantProperties.BASE_KEY, null, String.class, "tenant base key"),

    /**
     * Tenant token
     */
    TOKEN(TenantProperties.TOKEN, null, String.class, "tenant token");

    @Getter
    private final String key;

    @Getter
    private final Object defaultValue;

    @Getter
    private final Class clazz;

    @Getter
    private final String description;

    private QueryParams(String key, Object defaultValue, Class clazz, String description) {
        this.key = key;
        this.defaultValue = defaultValue;
        this.clazz = clazz;
        this.description = description;
    }

}
