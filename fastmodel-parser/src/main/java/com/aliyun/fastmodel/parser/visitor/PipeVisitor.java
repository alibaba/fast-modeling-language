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

import com.aliyun.fastmodel.core.tree.AliasedName;
import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.literal.BaseLiteral;
import com.aliyun.fastmodel.core.tree.statement.element.CreateElement;
import com.aliyun.fastmodel.core.tree.statement.pipe.CreatePipe;
import com.aliyun.fastmodel.core.tree.statement.pipe.PipeCopyInto;
import com.aliyun.fastmodel.core.tree.statement.pipe.PipeFrom;
import com.aliyun.fastmodel.core.tree.statement.pipe.PipeType;
import com.aliyun.fastmodel.core.tree.statement.pipe.TargetType;
import com.aliyun.fastmodel.core.tree.util.PropertyUtil;
import com.aliyun.fastmodel.parser.AstBuilder;
import com.aliyun.fastmodel.parser.annotation.SubVisitor;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.CopyIntoFromContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.CreatePipeContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.KeyValueContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.KeyValuePairsContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.TargetTypeContext;

/**
 * Builder
 *
 * @author panguanjing
 * @date 2021/4/6
 */
@SubVisitor
public class PipeVisitor extends AstBuilder {

    @Override
    public Node visitCreatePipe(CreatePipeContext ctx) {
        Comment comment = visitIfPresent(ctx.comment(), Comment.class).orElse(null);
        AliasedName aliasedName = visitIfPresent(ctx.alias(), AliasedName.class).orElse(null);
        CreateElement build = CreateElement.builder()
            .qualifiedName(getQualifiedName(ctx.qualifiedName()))
            .properties(getProperties(ctx.setProperties()))
            .comment(comment)
            .notExists(ctx.ifNotExists() != null)
            .aliasedName(aliasedName)
            .createOrReplace(ctx.replace() != null)
            .build();
        return new CreatePipe(
            build,
            getPipeType(ctx), getTarget(ctx),
            getFromConfig(ctx)
        );
    }

    @Override
    public Node visitKeyValue(KeyValueContext ctx) {
        String text = ctx.pipeKey().getText();
        BaseLiteral baseLiteral = (BaseLiteral)visit(ctx.constant());
        return new Property(text, baseLiteral);
    }

    private PipeFrom getFromConfig(CreatePipeContext ctx) {
        CopyIntoFromContext copyIntoFromContext = ctx.copyIntoFrom();
        BaseExpression baseExpression = (BaseExpression)visit(copyIntoFromContext.expression());
        PipeFrom pipeFrom = new PipeFrom();
        pipeFrom.setExpression(baseExpression);
        QualifiedName qualifiedName = getQualifiedName(copyIntoFromContext.tableName());
        pipeFrom.setFrom(qualifiedName);
        return pipeFrom;
    }

    private PipeCopyInto getTarget(CreatePipeContext ctx) {
        CopyIntoFromContext copyIntoFromContext = ctx.copyIntoFrom();
        KeyValuePairsContext copy = copyIntoFromContext.copy;
        PipeCopyInto targetConfig = new PipeCopyInto();
        if (copy != null) {
            List<KeyValueContext> keyValueContexts = copy.keyValue();
            List<Property> properties = visit(keyValueContexts, Property.class);
            targetConfig = PropertyUtil.toObject(properties, PipeCopyInto.class);
        }
        TargetTypeContext targetTypeContext = copyIntoFromContext.targetType();
        TargetType targetType = getTargetType(targetTypeContext);
        targetConfig.setTargetType(targetType);
        return targetConfig;
    }

    private TargetType getTargetType(TargetTypeContext targetTypeContext) {
        String text = targetTypeContext.getText();
        return TargetType.valueOf(text.toUpperCase());
    }

    private PipeType getPipeType(CreatePipeContext ctx) {
        //默认是异步的操作处理
        if (ctx.pipeType() == null) {
            return PipeType.ASYNC;
        }
        if (ctx.pipeType().KW_ASYNC() != null) {
            return PipeType.ASYNC;
        } else if (ctx.pipeType().KW_SYNC() != null) {
            return PipeType.SYNC;
        }
        throw new IllegalArgumentException("can't find pipeType:" + ctx.pipeType().getText());
    }
}
