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

import java.util.List;
import java.util.Optional;

import com.aliyun.fastmodel.core.tree.AliasedName;
import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.statement.constants.IndicatorType;
import com.aliyun.fastmodel.core.tree.statement.element.CreateElement;
import com.aliyun.fastmodel.core.tree.statement.indicator.CreateAtomicCompositeIndicator;
import com.aliyun.fastmodel.core.tree.statement.indicator.CreateAtomicIndicator;
import com.aliyun.fastmodel.core.tree.statement.indicator.CreateDerivativeCompositeIndicator;
import com.aliyun.fastmodel.core.tree.statement.indicator.CreateDerivativeIndicator;
import com.aliyun.fastmodel.core.tree.statement.indicator.DropIndicator;
import com.aliyun.fastmodel.core.tree.statement.indicator.RenameIndicator;
import com.aliyun.fastmodel.core.tree.statement.indicator.SetIndicatorAlias;
import com.aliyun.fastmodel.core.tree.statement.indicator.SetIndicatorComment;
import com.aliyun.fastmodel.core.tree.statement.indicator.SetIndicatorProperties;
import com.aliyun.fastmodel.parser.AstBuilder;
import com.aliyun.fastmodel.parser.annotation.SubVisitor;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.CreateIndicatorStatementContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.DropIndicatorStatementContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.IndicatorTypeContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.RenameIndicatorContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.SetIndicatorAliasContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.SetIndicatorCommentContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.SetIndicatorPropertiesContext;
import org.antlr.v4.runtime.tree.TerminalNode;

/**
 * 指标的visitor
 *
 * @author panguanjing
 * @date 2021/4/9
 */
@SubVisitor
public class IndicatorVisitor extends AstBuilder {
    @Override
    public Node visitCreateIndicatorStatement(CreateIndicatorStatementContext ctx) {
        BaseDataType baseDataType = visitIfPresent(ctx.typeDbCol(), BaseDataType.class).orElse(null);
        BaseExpression baseExpression = ctx.expression() != null ? (BaseExpression)visit(ctx.expression()) : null;
        List<Property> properties = getProperties(ctx.setProperties());
        Optional<Comment> comment = visitIfPresent(ctx.comment(), Comment.class);
        QualifiedName references = ctx.tableName() != null ? getQualifiedName(ctx.tableName()) : null;
        IndicatorType indicatorType = getIndicatorType(ctx.indicatorType());
        AliasedName aliasedName = visitIfPresent(ctx.alias(), AliasedName.class).orElse(null);
        CreateElement createElement = CreateElement.builder()
            .qualifiedName(getQualifiedName(ctx.qualifiedName()))
            .properties(properties)
            .comment(comment.orElse(null))
            .notExists(ctx.ifNotExists() != null)
            .aliasedName(aliasedName)
            .createOrReplace(ctx.replace() != null)
            .build();
        switch (indicatorType) {
            case ATOMIC:
                return new CreateAtomicIndicator(
                    createElement,
                    baseDataType,
                    baseExpression,
                    references
                );
            case ATOMIC_COMPOSITE:
                return new CreateAtomicCompositeIndicator(
                    createElement,
                    baseDataType,
                    baseExpression
                );
            case DERIVATIVE:
                return new CreateDerivativeIndicator(
                    createElement,
                    baseDataType,
                    baseExpression,
                    references
                );
            case DERIVATIVE_COMPOSITE:
                return new CreateDerivativeCompositeIndicator(
                    createElement,
                    baseDataType,
                    baseExpression,
                    references
                );
            default:
                break;
        }
        return super.visitCreateIndicatorStatement(ctx);
    }

    @Override
    public Node visitRenameIndicator(RenameIndicatorContext ctx) {
        return new RenameIndicator(getQualifiedName(ctx.qualifiedName()),
            getQualifiedName(ctx.alterStatementSuffixRename().qualifiedName()));
    }

    @Override
    public Node visitSetIndicatorComment(SetIndicatorCommentContext ctx) {
        Comment comment = (Comment)visit(ctx.alterStatementSuffixSetComment());
        return new SetIndicatorComment(getQualifiedName(ctx.qualifiedName()), comment);
    }

    @Override
    public Node visitSetIndicatorProperties(SetIndicatorPropertiesContext ctx) {
        List<Property> propertyList = getProperties(ctx.alterIndicatorSuffixProperties().setProperties());

        BaseDataType baseDataType = null;
        if (ctx.alterIndicatorSuffixProperties().colType() != null) {
            baseDataType = (BaseDataType)visit(ctx.alterIndicatorSuffixProperties().colType());
        }
        BaseExpression baseExpression = null;
        if (ctx.alterIndicatorSuffixProperties().expression() != null) {
            baseExpression = (BaseExpression)visit(ctx.alterIndicatorSuffixProperties().expression());
        }

        QualifiedName reference = null;
        if (ctx.alterIndicatorSuffixProperties().tableName() != null) {
            reference = getQualifiedName(ctx.alterIndicatorSuffixProperties().tableName());
        }
        return new SetIndicatorProperties(getQualifiedName(ctx.qualifiedName()),
            propertyList,
            baseExpression,
            reference,
            baseDataType
        );
    }

    @Override
    public Node visitDropIndicatorStatement(DropIndicatorStatementContext ctx) {
        return new DropIndicator(getQualifiedName(ctx.qualifiedName()));
    }

    private IndicatorType getIndicatorType(IndicatorTypeContext indicatorType) {
        if (indicatorType == null) {
            return IndicatorType.ATOMIC;
        }
        TerminalNode terminalNode = indicatorType.KW_ATOMIC();
        if (terminalNode != null) {
            if (indicatorType.KW_COMPOSITE() == null) {
                return IndicatorType.ATOMIC;
            }
            return IndicatorType.ATOMIC_COMPOSITE;
        }
        TerminalNode terminalNode1 = indicatorType.KW_DERIVATIVE();
        if (terminalNode1 != null) {
            if (indicatorType.KW_COMPOSITE() == null) {
                return IndicatorType.DERIVATIVE;
            }
            return IndicatorType.DERIVATIVE_COMPOSITE;
        }
        throw new IllegalArgumentException("can't find the indicatorType with text : " + indicatorType.getText());
    }

    @Override
    public Node visitSetIndicatorAlias(SetIndicatorAliasContext ctx) {
        return new SetIndicatorAlias(
            getQualifiedName(ctx.qualifiedName()),
            (AliasedName)visit(ctx.setAliasedName())
        );
    }
}
