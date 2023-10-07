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

import java.util.List;

import com.aliyun.fastmodel.core.parser.DomainLanguage;
import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.statement.domain.CreateDomain;
import com.aliyun.fastmodel.parser.NodeParser;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * 单元测试，支持下
 *
 * @author panguanjing
 * @date 2020/9/17
 */
public class MultiTest {

    NodeParser fastModelAntlrParser = new NodeParser();

    @Test
    public void testMultiCreateDomain() {
        String script = "create domain domain1; \r\n create domain domain2;";
        DomainLanguage domainLanguage = new DomainLanguage(script);
        List<BaseStatement> statements = fastModelAntlrParser.multiParse(domainLanguage);
        assertEquals(2, statements.size());
        for (BaseStatement statement : statements) {
            assertEquals(statement.getClass(), CreateDomain.class);
        }
    }

    @Test
    public void testMultiCreateTable() {
        String f = "create dim table a (b bigint comment 'comment'); \n create fact table c (d bigint comment 'c');";
        DomainLanguage domainLanguage = new DomainLanguage(f);
        List<BaseStatement> statements = fastModelAntlrParser.multiParse(domainLanguage);
        assertEquals(2, statements.size());

    }
}