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

package com.aliyun.aliyun.transform.zen.element;

import java.util.List;

/**
 * node Element
 *
 * @author panguanjing
 * @date 2021/7/16
 */
public interface NodeElement<T extends NodeElement> {

    /**
     * element name
     *
     * @return
     */
    String elementName();

    /**
     * set element name
     *
     * @param name
     */
    void setElementName(String name);

    /**
     * add attribute name
     *
     * @param className
     */
    void addAttributeName(String className);

    /**
     * get attribute name
     *
     * @return
     */
    List<String> getAttributeName();

    /**
     * merge node element
     *
     * @param nodeElement
     * @return
     */
    T merge(T nodeElement);

    /**
     * expand element
     *
     * @return
     */
    List<T> expand();
}
