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

package com.aliyun.fastmodel.core.parser;

import com.aliyun.fastmodel.core.exception.ParseException;
import com.aliyun.fastmodel.core.tree.Node;

/**
 * 一个通用的语言parser
 *
 * @author panguanjing
 * @date 2021/7/15
 */
public interface LanguageParser<T extends Node, C> {

    /**
     * parse node
     *
     * @param text
     * @param <T>
     * @return
     * @throws ParseException
     */
    public default <T> T parseNode(String text) throws ParseException {
        return parseNode(text, null);
    }

    /**
     * parse node with context
     *
     * @param text
     * @param context
     * @param <T>
     * @return
     * @throws ParseException
     */
    public <T> T parseNode(String text, C context) throws ParseException;
}
