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

package com.aliyun.fastmodel.parser;

import com.aliyun.fastmodel.core.parser.DomainLanguage;
import com.aliyun.fastmodel.core.tree.BaseStatement;

import static org.junit.Assert.assertEquals;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2020/12/2
 */
public abstract class BaseTest {

    protected NodeParser nodeParser = new NodeParser();

    public <T> T parse(String statement, Class<T> clazz) {
        BaseStatement parse = nodeParser.parse(new DomainLanguage(statement));
        return clazz.cast(parse);
    }

    public <T> T parseExpression(String expression) {
        return nodeParser.parseExpression(expression);
    }

    /**
     * 判断输入的操作是否相等
     *
     * @param fml
     * @param actual
     */
    public void assertParse(String fml, String expect) {
        BaseStatement parse = nodeParser.parse(new DomainLanguage(fml));
        assertEquals(expect, parse.toString());
    }
}

