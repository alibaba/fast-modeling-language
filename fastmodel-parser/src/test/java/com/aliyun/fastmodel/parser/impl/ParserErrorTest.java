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

package com.aliyun.fastmodel.parser.impl;

import com.aliyun.fastmodel.core.exception.ParseException;
import com.aliyun.fastmodel.core.parser.DomainLanguage;
import com.aliyun.fastmodel.parser.NodeParser;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

/**
 * 用于测试语法错误的情况
 *
 * @author panguanjing
 * @date 2020/9/3
 */
public class ParserErrorTest {

    NodeParser fastModelAntlrParser = new NodeParser();

    @Test
    public void testCreateIndicatorWithoutIdentifier() {
        String dsl = "create indicator";
        DomainLanguage domainLanguage = new DomainLanguage(dsl);
        try {
            fastModelAntlrParser.parse(domainLanguage);
        } catch (ParseException e) {
            assertNotNull(e);
        }
    }

}
