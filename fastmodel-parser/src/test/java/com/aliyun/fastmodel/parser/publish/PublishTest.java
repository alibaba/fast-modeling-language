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

package com.aliyun.fastmodel.parser.publish;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import com.aliyun.fastmodel.core.exception.ParseException;
import com.aliyun.fastmodel.core.parser.DomainLanguage;
import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.parser.NodeParser;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * 用于支持下
 *
 * @author panguanjing
 * @date 2020/10/13
 */
public class PublishTest {

    NodeParser fastModelParser = new NodeParser();

    @Test
    public void testPublish() throws IOException {
        String utf8 = IOUtils.toString(
            PublishTest.class.getResourceAsStream("/publish.sql"), StandardCharsets.UTF_8);
        DomainLanguage domainLanguage = new DomainLanguage(utf8);
        List<BaseStatement> statements = null;
        try {
            statements = fastModelParser.multiParse(domainLanguage);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        Assert.assertNotNull(statements);
    }
}
