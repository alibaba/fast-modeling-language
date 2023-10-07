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

package com.aliyun.aliyun.transform.zen.parser;

import com.aliyun.aliyun.transform.zen.parser.tree.AtomicZenExpression;
import com.aliyun.aliyun.transform.zen.parser.tree.AttributeZenExpression;
import com.aliyun.aliyun.transform.zen.parser.tree.BaseAtomicZenExpression;
import com.aliyun.aliyun.transform.zen.parser.tree.BaseZenExpression;
import com.aliyun.aliyun.transform.zen.parser.tree.BrotherZenExpression;
import com.aliyun.fastmodel.common.parser.ParserHelper;
import com.aliyun.fastmodel.common.utils.StripUtils;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.literal.StringLiteral;
import com.aliyun.fastmodel.transform.zen.parser.FastModelZenParser.AtomicContext;
import com.aliyun.fastmodel.transform.zen.parser.FastModelZenParser.AtomicExpressionContext;
import com.aliyun.fastmodel.transform.zen.parser.FastModelZenParser.BrotherContext;
import com.aliyun.fastmodel.transform.zen.parser.FastModelZenParser.IdentifierContext;
import com.aliyun.fastmodel.transform.zen.parser.FastModelZenParser.StringContext;
import com.aliyun.fastmodel.transform.zen.parser.FastModelZenParserBaseVisitor;
import org.antlr.v4.runtime.tree.TerminalNode;

import static com.aliyun.fastmodel.common.parser.ParserHelper.getLocation;
import static com.aliyun.fastmodel.common.parser.ParserHelper.getOrigin;

/**
 * ZenAstBuilder
 *
 * @author panguanjing
 * @date 2021/7/15
 */
public class ZenAstBuilder extends FastModelZenParserBaseVisitor<Node> {
    @Override
    public Node visitAtomic(AtomicContext ctx) {
        return visitAtomicExpression(ctx.atomicExpression());
    }

    @Override
    public Node visitAtomicExpression(AtomicExpressionContext ctx) {
        AtomicExpressionContext atomicExpressionContext = ctx.atomicExpression();
        if (atomicExpressionContext != null) {
            BaseAtomicZenExpression atomicZenExpression = (BaseAtomicZenExpression)visit(atomicExpressionContext);
            Identifier identifier = ParserHelper.visitIfPresent(this, ctx.identifier(), Identifier.class)
                .orElse(null);
            StringLiteral stringLiteral = ParserHelper.visitIfPresent(this, ctx.string(), StringLiteral.class).orElse(
                null);
            return new AttributeZenExpression(atomicZenExpression, identifier, stringLiteral);
        }
        TerminalNode terminalNode = ctx.INTEGER_VALUE();
        if (terminalNode == null) {
            return new AtomicZenExpression((Identifier)visit(ctx.identifier()), null);
        } else {
            return new AtomicZenExpression((Identifier)visit(ctx.identifier()), Long.parseLong(terminalNode.getText()));
        }
    }

    @Override
    public Node visitBrother(BrotherContext ctx) {
        return new BrotherZenExpression(
            (BaseZenExpression)visit(ctx.left),
            (BaseZenExpression)visit(ctx.right)
        );
    }

    @Override
    public Node visitString(StringContext ctx) {
        return new StringLiteral(getLocation(ctx), getOrigin(ctx), StripUtils.strip(ctx.getText()));
    }

    @Override
    public Node visitIdentifier(IdentifierContext ctx) {
        return ParserHelper.getIdentifier(ctx);
    }
}
