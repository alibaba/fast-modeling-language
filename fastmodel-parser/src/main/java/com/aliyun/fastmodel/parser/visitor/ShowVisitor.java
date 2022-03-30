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
import com.aliyun.fastmodel.core.tree.statement.show.WhereCondition;
import com.aliyun.fastmodel.core.tree.statement.showcreate.Output;
import com.aliyun.fastmodel.core.tree.statement.showcreate.ShowCreate;
import com.aliyun.fastmodel.parser.AstBuilder;
import com.aliyun.fastmodel.parser.annotation.SubVisitor;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.QualifiedNameContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.ShowCreateContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.ShowObjectsContext;

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
        QualifiedNameContext context = ctx.qualifiedName();
        QualifiedName qualifiedName = null;
        Identifier identifier = null;
        ShowObjectsType type = null;
        Identifier visit = (Identifier)visit(ctx.type);
        type = ShowObjectsType.getByCode(visit.getValue());
        if (context != null) {
            qualifiedName = getQualifiedName(context);
            if (isSubElement(type)) {
                identifier = qualifiedName.getFirstIdentifierIfSizeOverOne();
            } else {
                identifier = new Identifier(qualifiedName.getFirst());
            }
        }
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

    @Override
    public Node visitShowCreate(ShowCreateContext ctx) {
        Identifier visit = (Identifier)visit(ctx.showType());
        Output output = visitIfPresent(ctx.output(), Output.class).orElse(null);
        return new ShowCreate(
            getQualifiedName(ctx.qualifiedName()),
            ShowType.getByCode(visit.getValue()), output);
    }

}
