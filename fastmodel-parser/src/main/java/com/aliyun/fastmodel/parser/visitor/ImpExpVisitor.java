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

package com.aliyun.fastmodel.parser.visitor;

import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.literal.StringLiteral;
import com.aliyun.fastmodel.core.tree.statement.impexp.ExportStatement;
import com.aliyun.fastmodel.parser.AstBuilder;
import com.aliyun.fastmodel.parser.annotation.SubVisitor;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.ExportBusinessUnitContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.ExportStatementContext;

/**
 * ExportVisitor
 *
 * @author panguanjing
 * @date 2021/8/22
 */
@SubVisitor
public class ImpExpVisitor extends AstBuilder {
    @Override
    public Node visitExportStatement(ExportStatementContext ctx) {
        StringLiteral output = null;
        if (ctx.exportOutput() != null) {
            output = (StringLiteral)visit(ctx.exportOutput().string());
        }
        Identifier identifier = null;
        ExportBusinessUnitContext exportBusinessUnitContext = ctx.exportBusinessUnit();
        if (exportBusinessUnitContext != null) {
            identifier = (Identifier)visit(exportBusinessUnitContext.identifier());
        }
        StringLiteral target = null;
        if (ctx.exportTarget() != null) {
            target = (StringLiteral)visit(ctx.exportTarget().string());
        }
        return new ExportStatement(identifier,
            output,
            target,
            (BaseExpression)visit(ctx.expression())
        );
    }
}
