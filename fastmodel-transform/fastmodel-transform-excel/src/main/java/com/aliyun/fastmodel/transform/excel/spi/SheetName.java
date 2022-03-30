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

package com.aliyun.fastmodel.transform.excel.spi;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Preconditions;
import lombok.Getter;

/**
 * sheet的名字，是否普通的类，还是变量类
 *
 * @author panguanjing
 * @date 2021/5/16
 */
@Getter
public class SheetName {

    private final String name;

    private final boolean isVariable;

    private String target = null;

    private final Pattern pattern = Pattern.compile("\\$\\{(\\w+\\.?\\w+)}");

    public SheetName(String name) {
        Preconditions.checkNotNull(name, "name can't be null");
        this.name = name;
        Matcher matcher = pattern.matcher(name);
        isVariable = matcher.find();
        if (isVariable) {
            target = matcher.group(1);
        }
    }

}
