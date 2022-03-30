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

import java.util.ArrayList;
import java.util.List;

import com.aliyun.fastmodel.core.tree.AliasedName;
import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.atom.TableOrColumn;
import com.aliyun.fastmodel.core.tree.expr.literal.StringLiteral;
import com.aliyun.fastmodel.core.tree.statement.batch.AbstractBatchElement;
import com.aliyun.fastmodel.core.tree.statement.batch.CreateBatchDomain;
import com.aliyun.fastmodel.core.tree.statement.batch.CreateBatchNamingDict;
import com.aliyun.fastmodel.core.tree.statement.batch.CreateIndicatorBatch;
import com.aliyun.fastmodel.core.tree.statement.batch.element.DateField;
import com.aliyun.fastmodel.core.tree.statement.batch.element.DefaultAdjunct;
import com.aliyun.fastmodel.core.tree.statement.batch.element.DimPathElement;
import com.aliyun.fastmodel.core.tree.statement.batch.element.DimTableElement;
import com.aliyun.fastmodel.core.tree.statement.batch.element.DomainElement;
import com.aliyun.fastmodel.core.tree.statement.batch.element.FromTableElement;
import com.aliyun.fastmodel.core.tree.statement.batch.element.IndicatorDefine;
import com.aliyun.fastmodel.core.tree.statement.batch.element.NamingDictElement;
import com.aliyun.fastmodel.core.tree.statement.batch.element.TableList;
import com.aliyun.fastmodel.core.tree.statement.batch.element.TimePeriodElement;
import com.aliyun.fastmodel.core.tree.statement.element.CreateElement;
import com.aliyun.fastmodel.parser.AstBuilder;
import com.aliyun.fastmodel.parser.annotation.SubVisitor;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.BatchDictElementContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.CreateBatchDictContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.CreateBatchDomainContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.CreateBatchIndicatorContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.DateFieldContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.DefaultAdjunctContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.DimPathContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.DimTableContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.DomainElementContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.FromTableContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.IndicatorDefineContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.PathContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.ReplaceContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.TimePeriodContext;
import com.google.common.collect.ImmutableList;

/**
 * 批量操作的语法定义内容
 *
 * @author panguanjing
 * @date 2021/4/8
 */
@SubVisitor
public class BatchVisitor extends AstBuilder {

    @Override
    public Node visitCreateBatchIndicator(CreateBatchIndicatorContext ctx) {
        List<Property> properties = ImmutableList.of();
        if (ctx.setProperties() != null) {
            properties = visit(ctx.setProperties().tableProperties().keyValueProperty(), Property.class);
        }
        List<AbstractBatchElement> visit = visit(ctx.batchElement(), AbstractBatchElement.class);
        List<IndicatorDefine> indicatorDefine = new ArrayList<>();
        DimTableElement dimTableElement = null;
        DefaultAdjunct defaultAdjunct = null;
        FromTableElement fromTable = null;
        TimePeriodElement timePeriod = null;
        DimPathElement dimPathElement = null;
        DateField dateField = null;
        for (AbstractBatchElement element : visit) {
            if (element.getClass() == IndicatorDefine.class) {
                indicatorDefine.add((IndicatorDefine)element);
                continue;
            }
            if (element.getClass() == DimTableElement.class) {
                dimTableElement = (DimTableElement)element;
                continue;
            }
            if (element.getClass() == DefaultAdjunct.class) {
                defaultAdjunct = (DefaultAdjunct)element;
                continue;
            }
            if (element.getClass() == FromTableElement.class) {
                fromTable = (FromTableElement)element;
                continue;
            }
            if (element.getClass() == TimePeriodElement.class) {
                timePeriod = (TimePeriodElement)element;
                continue;
            }
            if (element.getClass() == DimPathElement.class) {
                dimPathElement = (DimPathElement)element;
                continue;
            }
            if (element.getClass() == DateField.class) {
                dateField = (DateField)element;
            }
        }
        //如果定义了指标，那么返回createIndicatorBatch对象
        return new CreateIndicatorBatch(getQualifiedName(ctx.qualifiedName()),
            indicatorDefine, defaultAdjunct, fromTable, dimTableElement,
            timePeriod, dimPathElement, dateField, properties);
    }

    @Override
    public Node visitCreateBatchDomain(CreateBatchDomainContext ctx) {
        List<Property> properties = ImmutableList.of();
        if (ctx.setProperties() != null) {
            properties = visit(ctx.setProperties().tableProperties().keyValueProperty(), Property.class);
        }
        List<DomainElement> list = visit(ctx.domainElement(), DomainElement.class);
        return new CreateBatchDomain(getQualifiedName(ctx.qualifiedName()),
            list, properties);
    }

    @Override
    public Node visitCreateBatchDict(CreateBatchDictContext ctx) {
        List<Property> properties = ImmutableList.of();
        if (ctx.setProperties() != null) {
            properties = visit(ctx.setProperties().tableProperties().keyValueProperty(), Property.class);
        }
        Comment comment = visitIfPresent(ctx.comment(), Comment.class).orElse(null);
        List<NamingDictElement> namingDictElementList = visit(ctx.batchDictElement(), NamingDictElement.class);
        ReplaceContext replace = ctx.replace();
        return new CreateBatchNamingDict(CreateElement.builder()
            .createOrReplace(replace != null)
            .qualifiedName(getQualifiedName(ctx.qualifiedName()))
            .comment(comment)
            .properties(properties)
            .build(), namingDictElementList);
    }

    @Override
    public Node visitBatchDictElement(BatchDictElementContext ctx) {
        List<Property> properties = ImmutableList.of();
        if (ctx.setProperties() != null) {
            properties = visit(ctx.setProperties().tableProperties().keyValueProperty(), Property.class);
        }
        return new NamingDictElement(
            (Identifier)visit(ctx.identifier()),
            visitIfPresent(ctx.alias(), AliasedName.class).orElse(null),
            visitIfPresent(ctx.comment(), Comment.class).orElse(null),
            properties
        );
    }

    @Override
    public Node visitTimePeriod(TimePeriodContext ctx) {
        return new TimePeriodElement((Identifier)visit(ctx.identifier()));
    }

    @Override
    public Node visitDimTable(DimTableContext ctx) {
        return new DimTableElement(visit(ctx.identifier(), Identifier.class));
    }

    @Override
    public Node visitFromTable(FromTableContext ctx) {
        return new FromTableElement(visit(ctx.identifier(), Identifier.class));
    }

    @Override
    public Node visitDimPath(DimPathContext ctx) {
        return new DimPathElement(visit(ctx.path(), TableList.class));
    }

    @Override
    public Node visitPath(PathContext ctx) {
        return new TableList(visit(ctx.identifier(), Identifier.class));
    }

    @Override
    public Node visitDomainElement(DomainElementContext ctx) {
        return new DomainElement(
            (Identifier)visit(ctx.identifier()),
            visitIfPresent(ctx.comment(), Comment.class).orElse(null),
            getProperties(ctx.setProperties())
        );
    }

    @Override
    public Node visitDefaultAdjunct(DefaultAdjunctContext ctx) {
        return new DefaultAdjunct(
            visit(ctx.identifier(), Identifier.class)
        );
    }

    @Override
    public Node visitDateField(DateFieldContext ctx) {
        return new DateField(
            (TableOrColumn)visit(ctx.tableOrColumn()),
            ((StringLiteral)visit(ctx.string())).getValue()
        );
    }

    @Override
    public Node visitIndicatorDefine(IndicatorDefineContext ctx) {
        List<Identifier> visit = ImmutableList.of();
        if (ctx.adjunct() != null) {
            visit = visit(ctx.adjunct().identifier(), Identifier.class);
        }
        return new IndicatorDefine(
            (Identifier)visit(ctx.define),
            visitIfPresent(ctx.comment(), Comment.class).orElse(null),
            visit,
            (Identifier)visit(ctx.atomic),
            (BaseExpression)visit(ctx.expression())
        );
    }

}
