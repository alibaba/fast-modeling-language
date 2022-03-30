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

package com.aliyun.fastmodel.transform.graph.domain;

import java.util.Map;
import java.util.Set;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/12/12
 */
public interface Element {

    /**
     * element id
     *
     * @return
     */
    String id();

    /**
     * element label
     *
     * @return
     */
    String label();

    /**
     * attributes
     *
     * @return
     */
    Map<String, Object> properties();

    /**
     * get property with key
     *
     * @param property
     * @param <T>
     * @return
     */
    <T> T getProperty(String property);

    /**
     * set property
     *
     * @param key
     * @param value
     */
    void setProperty(String key, Object value);

    /**
     * remove key
     *
     * @param key
     */
    void removeProperty(String key);

    /**
     * get properties key
     *
     * @return
     */
    Set<String> getPropertyKeys();
}
