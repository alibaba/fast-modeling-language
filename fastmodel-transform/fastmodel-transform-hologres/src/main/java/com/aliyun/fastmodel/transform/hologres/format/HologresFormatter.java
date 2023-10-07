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

package com.aliyun.fastmodel.transform.hologres.format;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.transform.api.dialect.DialectNode;
import com.aliyun.fastmodel.transform.hologres.context.HologresTransformContext;
import com.aliyun.fastmodel.transform.hologres.dialect.HologresVersion;
import com.aliyun.fastmodel.transform.hologres.parser.visitor.HologresAstVisitor;
import org.apache.commons.lang3.StringUtils;

import static com.aliyun.fastmodel.transform.api.context.TransformContext.SEMICOLON;

/**
 * Hologres Formatter
 *
 * @author panguanjing
 * @date 2021/3/8
 */
public class HologresFormatter {

    public static DialectNode format(BaseStatement statement, HologresTransformContext context,
        HologresVersion version) {
        HologresAstVisitor hologresVisitor = new HologresAstVisitor(context, version);
        Boolean process = hologresVisitor.process(statement, 0);
        StringBuilder builder = hologresVisitor.getBuilder();
        String result = builder.toString();
        if (StringUtils.isBlank(result)) {
            return new DialectNode(result, process);
        }
        if (context.isAppendSemicolon() && !result.endsWith(SEMICOLON)) {
            result = result + SEMICOLON;
        }
        return new DialectNode(result, process);
    }

}

