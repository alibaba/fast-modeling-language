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

package com.aliyun.fastmodel.transform.plantuml.parser;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.transform.plantuml.exception.VisualParseException;

/**
 * parser
 *
 * @author panguanjing
 * @date 2020/9/18
 */
public interface FragmentParser {

    /**
     * 根据语句解析为片段内容
     *
     * @param statement 创建语句
     * @return 片段
     * @throws VisualParseException 如果可视化出现异常
     */
    Fragment parse(BaseStatement statement) throws VisualParseException;
}
