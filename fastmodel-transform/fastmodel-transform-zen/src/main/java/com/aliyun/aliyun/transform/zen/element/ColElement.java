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

import java.util.ArrayList;
import java.util.List;

import com.google.common.collect.Lists;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

/**
 * ColElement
 *
 * @author panguanjing
 * @date 2021/7/16
 */
@Getter
@Setter
@EqualsAndHashCode
public class ColElement implements NodeElement<ColElement> {
    private String colName;
    private List<String> classList;
    private List<ColElement> childColElements;

    @Override
    public String elementName() {
        return colName;
    }

    @Override
    public void setElementName(String name) {
        colName = name;
    }

    @Override
    public void addAttributeName(String className) {
        if (classList == null) {
            classList = Lists.newArrayList();
        }
        if (!classList.contains(className)) {
            classList.add(className);
        }
    }

    @Override
    public List<String> getAttributeName() {
        return classList;
    }

    @Override
    public ColElement merge(ColElement nodeElement) {
        if (childColElements == null) {
            childColElements = Lists.newArrayList();
        }
        if (!childColElements.contains(nodeElement)) {
            childColElements.add(nodeElement);
        }
        return this;
    }

    @Override
    public List<ColElement> expand() {
        List<ColElement> allElement = new ArrayList<>();
        allElement.add(this);
        List<ColElement> childColElements = getChildColElements();
        if (childColElements == null) {
            return allElement;
        }
        for (ColElement colElement : childColElements) {
            allElement.addAll(colElement.expand());
        }
        return allElement;
    }
}
