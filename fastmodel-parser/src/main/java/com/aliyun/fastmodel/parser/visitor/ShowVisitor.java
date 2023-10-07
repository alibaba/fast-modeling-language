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

import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.literal.LongLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.StringLiteral;
import com.aliyun.fastmodel.core.tree.statement.constants.ShowObjectsType;
import com.aliyun.fastmodel.core.tree.statement.constants.ShowType;
import com.aliyun.fastmodel.core.tree.statement.select.Limit;
import com.aliyun.fastmodel.core.tree.statement.select.Offset;
import com.aliyun.fastmodel.core.tree.statement.show.ConditionElement;
import com.aliyun.fastmodel.core.tree.statement.show.LikeCondition;
import com.aliyun.fastmodel.core.tree.statement.show.ShowObjects;
import com.aliyun.fastmodel.core.tree.statement.show.ShowSingleStatistic;
import com.aliyun.fastmodel.core.tree.statement.show.ShowStatistic;
import com.aliyun.fastmodel.core.tree.statement.show.WhereCondition;
import com.aliyun.fastmodel.core.tree.statement.showcreate.Output;
import com.aliyun.fastmodel.core.tree.statement.showcreate.ShowCreate;
import com.aliyun.fastmodel.parser.AstBuilder;
import com.aliyun.fastmodel.parser.annotation.SubVisitor;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.QualifiedNameContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.ShowCreateContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.ShowObjectsContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.ShowStatisticContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.ShowTypeContext;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;

import static com.aliyun.fastmodel.common.parser.ParserHelper.getLocation;
import static com.aliyun.fastmodel.common.parser.ParserHelper.getOrigin;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/4/9
 */
@SubVisitor
public class ShowVisitor extends AstBuilder {

    /**
     * 是否二级元素内容
     *
     * @param showObjectsType
     * @return
     */
    private boolean isSubElement(ShowObjectsType showObjectsType) {
        return showObjectsType == ShowObjectsType.COLUMNS || showObjectsType == ShowObjectsType.CODES || showObjectsType == ShowObjectsType.DIM_ATTRIBUTES;
    }

    @Override
    public Node visitShowObjects(ShowObjectsContext ctx) {
        ConditionElement conditionElement = null;
        if (ctx.KW_LIKE() != null) {
            StringLiteral stringLiteral = (StringLiteral)visit(ctx.string());
            conditionElement = new LikeCondition(stringLiteral.getValue());
        } else if (ctx.KW_WHERE() != null) {
            conditionElement = new WhereCondition((BaseExpression)visit(ctx.expression()));
        }

        Identifier visit = (Identifier)visit(ctx.type);
        List<QualifiedName> qualifiedName = getQualifiedNames(ctx);

        ShowObjectsType type = ShowObjectsType.getByCode(visit.getValue());
        Identifier identifier = getIdentifier(qualifiedName, type);

        Optional<Offset> offset = Optional.empty();
        Optional<Limit> limit = Optional.empty();
        if (ctx.KW_OFFSET() != null) {
            offset = Optional.of(new Offset(new LongLiteral(ctx.offset.getText())));
        }
        if (ctx.KW_LIMIT() != null) {
            limit = Optional.of(new Limit(new LongLiteral(ctx.limit.getText())));
        }
        return new ShowObjects(
            getLocation(ctx),
            getOrigin(ctx),
            conditionElement,
            ctx.KW_FULL() != null,
            identifier,
            type,
            qualifiedName,
            offset.orElse(null),
            limit.orElse(null)
        );
    }

    private Identifier getIdentifier(List<QualifiedName> qualifiedName, ShowObjectsType type) {
        if (CollectionUtils.isEmpty(qualifiedName)) {
            return null;
        }
        Identifier identifier = null;
        QualifiedName first = qualifiedName.get(0);
        if (isSubElement(type)) {
            identifier = first.getFirstIdentifierIfSizeOverOne();
        } else {
            identifier = new Identifier(first.getFirst());
        }
        return identifier;
    }

    private List<QualifiedName> getQualifiedNames(ShowObjectsContext ctx) {
        List<QualifiedNameContext> context = ctx.qualifiedName();
        List<QualifiedName> qualifiedName = Lists.newArrayList();
        if (CollectionUtils.isNotEmpty(context)) {
            for (QualifiedNameContext qualifiedNameContext : context) {
                QualifiedName name = getQualifiedName(qualifiedNameContext);
                qualifiedName.add(name);
            }
        }
        return qualifiedName;
    }

    @Override
    public Node visitShowStatistic(ShowStatisticContext ctx) {
        if (ctx.singleStatisticObject() != null) {
            ShowTypeContext showTypeContext = ctx.singleStatisticObject().showType();
            Identifier visit = (Identifier)visit(showTypeContext);
            QualifiedName qualifiedName = getQualifiedName(ctx.singleStatisticObject().qualifiedName());
            return new ShowSingleStatistic(
                getLocation(ctx),
                getOrigin(ctx),
                null,
                ShowType.getByCode(visit.getValue()),
                qualifiedName
            );
        } else {
            ShowObjectsType type = null;
            Identifier visit = (Identifier)visit(ctx.showObjectTypes());
            type = ShowObjectsType.getByCode(visit.getValue());
            return new ShowStatistic(getLocation(ctx), getOrigin(ctx), null, type);
        }
    }

    @Override
    public Node visitShowCreate(ShowCreateContext ctx) {
        Identifier visit = (Identifier)visit(ctx.showType());
        Output output = visitIfPresent(ctx.output(), Output.class).orElse(null);
        return new ShowCreate(
            getQualifiedName(ctx.qualifiedName()),
            ShowType.getByCode(visit.getValue()), output);
    }

}
