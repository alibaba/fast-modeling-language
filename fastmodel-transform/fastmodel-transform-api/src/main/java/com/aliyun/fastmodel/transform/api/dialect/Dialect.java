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

package com.aliyun.fastmodel.transform.api.dialect;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.TYPE;

/**
 * @author panguanjing
 * @date 2020/10/16
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(TYPE)
public @interface Dialect {
    /**
     * engine的名称
     *
     * @return engine的名字
     */
    String value();

    /**
     * 方言的版本
     *
     * @return
     */
    String version() default "";

    /**
     * 默认的方言
     */
    boolean defaultDialect() default false;

}
