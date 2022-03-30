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
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.similar.BetweenPredicate;
import com.aliyun.fastmodel.core.tree.statement.element.CreateElement;
import com.aliyun.fastmodel.core.tree.statement.timeperiod.CreateTimePeriod;
import com.aliyun.fastmodel.core.tree.statement.timeperiod.DropTimePeriod;
import com.aliyun.fastmodel.core.tree.statement.timeperiod.RenameTimePeriod;
import com.aliyun.fastmodel.core.tree.statement.timeperiod.SetTimePeriodAlias;
import com.aliyun.fastmodel.core.tree.statement.timeperiod.SetTimePeriodComment;
import com.aliyun.fastmodel.core.tree.statement.timeperiod.SetTimePeriodProperties;
import com.aliyun.fastmodel.parser.AstBuilder;
import com.aliyun.fastmodel.parser.annotation.SubVisitor;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.CreateTimePeriodContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.DropTimePeriodContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.RenameTimePeriodContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.SetTimePeriodAliasContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.SetTimePeriodCommentContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.SetTimePeriodPropertiesContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.TimePeriodExpressionContext;

import static com.aliyun.fastmodel.common.parser.ParserHelper.getLocation;
import static com.aliyun.fastmodel.common.parser.ParserHelper.getOrigin;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/4/8
 */
@SubVisitor
public class TimePeriodVisitor extends AstBuilder {

    @Override
    public Node visitCreateTimePeriod(CreateTimePeriodContext ctx) {
        List<Property> properties = getProperties(ctx.setProperties());
        TimePeriodExpressionContext tree = ctx.timePeriodExpression();
        BetweenPredicate betweenPredicate = null;
        if (tree != null) {
            betweenPredicate = (BetweenPredicate)visit(tree);
        }
        Comment visit = visitIfPresent(ctx.comment(), Comment.class).orElse(null);
        AliasedName aliasedName = visitIfPresent(ctx.alias(), AliasedName.class).orElse(null);
        CreateElement createElement = CreateElement.builder()
            .qualifiedName(getQualifiedName(ctx.qualifiedName()))
            .properties(properties)
            .comment(visit)
            .notExists(ctx.ifNotExists() != null)
            .createOrReplace(ctx.replace() != null)
            .aliasedName(aliasedName)
            .build();
        return new CreateTimePeriod(
            createElement,
            betweenPredicate
        );
    }

    @Override
    public Node visitTimePeriodExpression(TimePeriodExpressionContext ctx) {
        return new BetweenPredicate(getLocation(ctx), getOrigin(ctx), null, (BaseExpression)visit(ctx.lower),
            (BaseExpression)visit(ctx.upper));
    }

    @Override
    public Node visitRenameTimePeriod(RenameTimePeriodContext ctx) {
        return new RenameTimePeriod(
            getQualifiedName(ctx.qualifiedName()),
            getQualifiedName(ctx.alterStatementSuffixRename().qualifiedName())
        );
    }

    @Override
    public Node visitSetTimePeriodComment(SetTimePeriodCommentContext ctx) {
        return new SetTimePeriodComment(
            getQualifiedName(ctx.qualifiedName()),
            (Comment)visit(ctx.alterStatementSuffixSetComment())
        );
    }

    @Override
    public Node visitSetTimePeriodProperties(SetTimePeriodPropertiesContext ctx) {
        List<Property> list = getProperties(ctx.setProperties());
        BetweenPredicate visit = visitIfPresent(ctx.timePeriodExpression(), BetweenPredicate.class).orElse(null);
        return new SetTimePeriodProperties(
            getQualifiedName(ctx.qualifiedName()),
            list,
            visit
        );
    }

    @Override
    public Node visitDropTimePeriod(DropTimePeriodContext ctx) {
        return new DropTimePeriod(
            getQualifiedName(ctx.qualifiedName())
        );
    }

    @Override
    public Node visitSetTimePeriodAlias(SetTimePeriodAliasContext ctx) {
        return new SetTimePeriodAlias(getQualifiedName(ctx.qualifiedName()), (AliasedName)visit(ctx.setAliasedName()));
    }
}
