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

package com.aliyun.fastmodel.transform.fml.format;

import com.aliyun.fastmodel.core.formatter.ExpressionVisitor;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.util.IdentifierUtil;
import com.aliyun.fastmodel.parser.lexer.ReservedIdentifier;

/**
 * Fml Expression Visitor
 * 扩展了默认的表达式visitor，增加按照关键字转义方法
 * 比如：
 * following -> `following`
 *
 * @author panguanjing
 * @date 2022/1/9
 */
public class FmlExpressionVisitor extends ExpressionVisitor {

    @Override
    public String visitIdentifier(Identifier node, Void context) {
        if (node.isDelimited()) {
            return super.visitIdentifier(node, context);
        }
        String value = node.getValue();
        //如果是关键字，那么增加转义处理
        if (ReservedIdentifier.isKeyWord(value)) {
            return IdentifierUtil.delimit(value);
        }
        return super.visitIdentifier(node, context);
    }
}
