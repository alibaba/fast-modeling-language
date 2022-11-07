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

import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import com.aliyun.fastmodel.common.parser.ParserHelper;
import com.aliyun.fastmodel.core.tree.BaseRelation;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.AllRows;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.Parameter;
import com.aliyun.fastmodel.core.tree.expr.atom.GroupingOperation;
import com.aliyun.fastmodel.core.tree.expr.literal.LongLiteral;
import com.aliyun.fastmodel.core.tree.relation.AliasedRelation;
import com.aliyun.fastmodel.core.tree.relation.Join;
import com.aliyun.fastmodel.core.tree.relation.SampledRelation;
import com.aliyun.fastmodel.core.tree.relation.SampledType;
import com.aliyun.fastmodel.core.tree.relation.join.JoinCriteria;
import com.aliyun.fastmodel.core.tree.relation.join.JoinOn;
import com.aliyun.fastmodel.core.tree.relation.join.JoinToken;
import com.aliyun.fastmodel.core.tree.relation.join.JoinUsing;
import com.aliyun.fastmodel.core.tree.relation.join.NaturalJoin;
import com.aliyun.fastmodel.core.tree.relation.querybody.BaseQueryBody;
import com.aliyun.fastmodel.core.tree.relation.querybody.Except;
import com.aliyun.fastmodel.core.tree.relation.querybody.Intersect;
import com.aliyun.fastmodel.core.tree.relation.querybody.QuerySpecification;
import com.aliyun.fastmodel.core.tree.relation.querybody.Table;
import com.aliyun.fastmodel.core.tree.relation.querybody.TableSubQuery;
import com.aliyun.fastmodel.core.tree.relation.querybody.Union;
import com.aliyun.fastmodel.core.tree.relation.querybody.Values;
import com.aliyun.fastmodel.core.tree.statement.constants.DeleteType;
import com.aliyun.fastmodel.core.tree.statement.delete.Delete;
import com.aliyun.fastmodel.core.tree.statement.insert.Insert;
import com.aliyun.fastmodel.core.tree.statement.rule.PartitionSpec;
import com.aliyun.fastmodel.core.tree.statement.select.FetchFirst;
import com.aliyun.fastmodel.core.tree.statement.select.Hint;
import com.aliyun.fastmodel.core.tree.statement.select.Limit;
import com.aliyun.fastmodel.core.tree.statement.select.Offset;
import com.aliyun.fastmodel.core.tree.statement.select.Query;
import com.aliyun.fastmodel.core.tree.statement.select.Select;
import com.aliyun.fastmodel.core.tree.statement.select.With;
import com.aliyun.fastmodel.core.tree.statement.select.WithQuery;
import com.aliyun.fastmodel.core.tree.statement.select.groupby.Cube;
import com.aliyun.fastmodel.core.tree.statement.select.groupby.GroupBy;
import com.aliyun.fastmodel.core.tree.statement.select.groupby.GroupingElement;
import com.aliyun.fastmodel.core.tree.statement.select.groupby.GroupingSets;
import com.aliyun.fastmodel.core.tree.statement.select.groupby.Rollup;
import com.aliyun.fastmodel.core.tree.statement.select.groupby.SimpleGroupBy;
import com.aliyun.fastmodel.core.tree.statement.select.item.AllColumns;
import com.aliyun.fastmodel.core.tree.statement.select.item.SelectItem;
import com.aliyun.fastmodel.core.tree.statement.select.item.SingleColumn;
import com.aliyun.fastmodel.core.tree.statement.select.order.NullOrdering;
import com.aliyun.fastmodel.core.tree.statement.select.order.OrderBy;
import com.aliyun.fastmodel.core.tree.statement.select.order.Ordering;
import com.aliyun.fastmodel.core.tree.statement.select.order.SortItem;
import com.aliyun.fastmodel.core.tree.statement.select.sort.ClusterBy;
import com.aliyun.fastmodel.core.tree.statement.select.sort.DistributeBy;
import com.aliyun.fastmodel.core.tree.statement.select.sort.SortBy;
import com.aliyun.fastmodel.core.tree.statement.show.WhereCondition;
import com.aliyun.fastmodel.parser.AstBuilder;
import com.aliyun.fastmodel.parser.annotation.SubVisitor;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.AliasedRelationContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.ClusterByClauseContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.CubeContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.DeleteContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.DistributeByClauseContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.GroupByContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.GroupingOperationContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.HintStatementContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.InlineTableContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.InsertIntoContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.JoinRelationContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.MultipleGroupingSetsContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.NamedQueryContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.ParenthesizedRelationContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.QueryContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.QueryNoWithContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.QuerySpecificationContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.RelationDefaultContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.RollupContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.SampledRelationContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.SelectAllContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.SelectSingleContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.SetOperationContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.SetQuantifierContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.SingleGroupingSetContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.SortByClauseContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.SortItemContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.SubqueryContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.SubqueryRelationContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.TableContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.WithContext;
import com.aliyun.fastmodel.parser.generate.FastModelLexer;
import com.google.common.collect.ImmutableList;
import org.antlr.v4.runtime.Token;

import static com.aliyun.fastmodel.common.parser.ParserHelper.getLocation;
import static com.aliyun.fastmodel.common.parser.ParserHelper.getOrigin;
import static java.util.stream.Collectors.toList;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/4/9
 */
@SubVisitor
public class QueryVisitor extends AstBuilder {

    private int parameterPosition;

    @Override
    public Node visitQuery(QueryContext ctx) {
        Query body = (Query)visit(ctx.queryNoWith());

        return new Query(
            visitIfPresent(ctx.with(), With.class).orElse(null),
            body.getQueryBody(),
            body.getOrderBy(),
            body.getOffset(),
            body.getLimit());
    }

    @Override
    public Node visitWith(WithContext ctx) {
        return new With(ParserHelper.getLocation(ctx),
            ctx.KW_RECURSIVE() != null, visit(ctx.namedQuery(), WithQuery.class));
    }

    @Override
    public Node visitQueryNoWith(QueryNoWithContext ctx) {
        BaseQueryBody term = (BaseQueryBody)visit(ctx.queryTerm());

        Optional<OrderBy> orderBy = Optional.empty();
        if (ctx.KW_ORDER() != null) {
            orderBy = Optional.of(new OrderBy(visit(ctx.sortItem(), SortItem.class)));
        }

        ClusterBy clusterBy = null;
        if (ctx.clusterByClause() != null) {
            clusterBy = (ClusterBy)visit(ctx.clusterByClause());
        }

        DistributeBy distributeBy = null;
        if (ctx.distributeByClause() != null) {
            distributeBy = (DistributeBy)visit(ctx.distributeByClause());
        }

        SortBy sortBy = null;
        if (ctx.sortByClause() != null) {
            sortBy = (SortBy)visit(ctx.sortByClause());
        }

        Optional<Offset> offset = Optional.empty();
        if (ctx.KW_OFFSET() != null) {
            BaseExpression rowCount;
            if (ctx.offset.INTEGER_VALUE() != null) {
                rowCount = new LongLiteral(ctx.offset.getText());
            } else {
                rowCount = new Parameter(ParserHelper.getLocation(ctx.offset.QUESTION()),
                    getOrigin(ctx.offset.QUESTION()), parameterPosition);
                parameterPosition++;
            }
            offset = Optional.of(new Offset(ParserHelper.getLocation(ctx.KW_OFFSET()), rowCount));
        }

        Optional<Node> limit = Optional.empty();
        if (ctx.KW_FETCH() != null) {
            Optional<BaseExpression> rowCount = Optional.empty();
            if (ctx.fetchFirst != null) {
                if (ctx.fetchFirst.INTEGER_VALUE() != null) {
                    rowCount = Optional.of(new LongLiteral(ctx.fetchFirst.getText()));
                } else {
                    rowCount = Optional.of(
                        new Parameter(ParserHelper.getLocation(ctx.fetchFirst.QUESTION()),
                            getOrigin(ctx.fetchFirst.QUESTION()), parameterPosition));
                    parameterPosition++;
                }
            }
            limit = Optional.of(
                new FetchFirst(ParserHelper.getLocation(ctx.KW_FETCH()), rowCount.orElse(null), ctx.KW_TIES() != null));
        } else if (ctx.KW_LIMIT() != null) {
            if (ctx.limit == null) {
                throw new IllegalStateException("Missing LIMIT value");
            }
            BaseExpression rowCount;
            if (ctx.limit.KW_ALL() != null) {
                rowCount = new AllRows(ParserHelper.getLocation(ctx.limit.KW_ALL()), getOrigin(ctx.limit.KW_ALL()));
            } else if (ctx.limit.rowCount().INTEGER_VALUE() != null) {
                rowCount = new LongLiteral(ctx.limit.getText());
            } else {
                rowCount = new Parameter(ParserHelper.getLocation(ctx.limit.rowCount().QUESTION()),
                    getOrigin(ctx.limit.rowCount().QUESTION()), parameterPosition);
                parameterPosition++;
            }

            limit = Optional.of(new Limit(ParserHelper.getLocation(ctx.KW_LIMIT()), rowCount));
        }

        if (term instanceof QuerySpecification) {
            // When we have a simple query specification
            // followed by order by, offset, limit or fetch,
            // fold the order by, limit, offset or fetch clauses
            // into the query specification (analyzer/planner
            // expects this structure to resolve references with respect
            // to columns defined in the query specification)
            QuerySpecification query = (QuerySpecification)term;

            return new Query(
                null,
                new QuerySpecification(
                    query.getSelect(),
                    query.getHints(),
                    query.getFrom(),
                    query.getWhere(),
                    query.getGroupBy(),
                    query.getHaving(),
                    orderBy.orElse(null),
                    clusterBy,
                    distributeBy,
                    sortBy,
                    offset.orElse(null),
                    limit.orElse(null)),
                null,
                null,
                null);
        }

        return new Query(
            null,
            term,
            orderBy.orElse(null),
            offset.orElse(null),
            limit.orElse(null));
    }

    @Override
    public Node visitClusterByClause(ClusterByClauseContext ctx) {
        return new ClusterBy(
            visit(ctx.expression(), BaseExpression.class)
        );
    }

    @Override
    public Node visitDistributeByClause(DistributeByClauseContext ctx) {
        return new DistributeBy(
            visit(ctx.sortItem(), SortItem.class)
        );
    }

    @Override
    public Node visitSortByClause(SortByClauseContext ctx) {
        return new SortBy(
            visit(ctx.sortItem(), SortItem.class)
        );
    }

    @Override
    public Node visitSetOperation(SetOperationContext ctx) {
        BaseQueryBody left = (BaseQueryBody)visit(ctx.left);
        BaseQueryBody right = (BaseQueryBody)visit(ctx.right);

        boolean distinct = ctx.setQuantifier() == null || ctx.setQuantifier().KW_DISTINCT() != null;
        switch (ctx.operator.getType()) {
            case FastModelLexer.KW_UNION:
                return new Union(ImmutableList.of(left, right), distinct);
            case FastModelLexer.KW_INTERSECT:
                return new Intersect(ImmutableList.of(left, right), distinct);
            case FastModelLexer.KW_EXCEPT:
                return new Except(left, right, distinct);
            default:
                throw new IllegalArgumentException("Unsupported set operation: " + ctx.operator.getText());
        }
    }

    @Override
    public Node visitTable(TableContext ctx) {
        return new Table(getQualifiedName(ctx.qualifiedName()));
    }

    @Override
    public Node visitInlineTable(InlineTableContext ctx) {
        return new Values(visit(ctx.expression(), BaseExpression.class));
    }

    @Override
    public Node visitSubquery(SubqueryContext ctx) {
        return new TableSubQuery((Query)visit(ctx.queryNoWith()));
    }

    @Override
    public Node visitSortItem(SortItemContext ctx) {
        return new SortItem(
            ParserHelper.getLocation(ctx),
            (BaseExpression)visit(ctx.expression()),
            Optional.ofNullable(ctx.ordering)
                .map(this::getOrderingType)
                .orElse(null),
            Optional.ofNullable(ctx.nullOrdering)
                .map(this::getNullOrderingType)
                .orElse(NullOrdering.UNDEFINED));
    }

    @Override
    public Node visitQuerySpecification(QuerySpecificationContext ctx) {
        Optional<BaseRelation> from = Optional.empty();
        List<SelectItem> selectItems = visit(ctx.selectItem(), SelectItem.class);

        List<BaseRelation> relations = visit(ctx.relation(), BaseRelation.class);
        if (!relations.isEmpty()) {
            // synthesize implicit join nodes
            Iterator<BaseRelation> iterator = relations.iterator();
            BaseRelation relation = iterator.next();

            while (iterator.hasNext()) {
                relation = new Join(ParserHelper.getLocation(ctx), JoinToken.IMPLICIT, relation, iterator.next(), null);
            }
            from = Optional.of(relation);
        }
        List<Hint> hints = ImmutableList.of();
        if (ctx.hints != null) {
            hints = visit(ctx.hints.hintStatement(), Hint.class);
        }
        return new QuerySpecification(
            new Select(ParserHelper.getLocation(ctx.KW_SELECT()), selectItems, isDistinct(ctx.setQuantifier())),
            hints,
            from.orElse(null),
            visitIfPresent(ctx.where, BaseExpression.class).orElse(null),
            visitIfPresent(ctx.groupBy(), GroupBy.class).orElse(null),
            visitIfPresent(ctx.having, BaseExpression.class).orElse(null),
            null,
            null,
            null,
            null,
            null,
            null);
    }

    @Override
    public Node visitHintStatement(HintStatementContext ctx) {
        List<BaseExpression> list = ImmutableList.of();
        if (ctx.parameters != null) {
            list = visit(ctx.parameters, BaseExpression.class);
        }
        return new Hint((Identifier)visit(ctx.hintName), list);
    }

    @Override
    public Node visitGroupBy(GroupByContext ctx) {
        return new GroupBy(ParserHelper.getLocation(ctx),
            isDistinct(ctx.setQuantifier()),
            visit(ctx.groupingElement(), GroupingElement.class));
    }

    @Override
    public Node visitSingleGroupingSet(SingleGroupingSetContext ctx) {
        return new SimpleGroupBy(visit(ctx.groupingSet().expression(), BaseExpression.class));
    }

    @Override
    public Node visitRollup(RollupContext ctx) {
        return new Rollup(visit(ctx.expression(), BaseExpression.class));
    }

    @Override
    public Node visitCube(CubeContext ctx) {
        return new Cube(visit(ctx.expression(), BaseExpression.class));
    }

    @Override
    public Node visitMultipleGroupingSets(MultipleGroupingSetsContext ctx) {
        return new GroupingSets(ctx.groupingSet().stream()
            .map(groupingSet -> visit(groupingSet.expression(), BaseExpression.class))
            .collect(toList()));
    }

    @Override
    public Node visitNamedQuery(NamedQueryContext ctx) {
        Optional<List<Identifier>> columns = Optional.empty();
        if (ctx.columnAliases() != null) {
            columns = Optional.of(visit(ctx.columnAliases().identifier(), Identifier.class));
        }

        return new WithQuery(
            (Identifier)visit(ctx.name),
            (Query)visit(ctx.query()),
            columns.orElse(null));
    }

    @Override
    public Node visitSelectSingle(SelectSingleContext ctx) {
        return new SingleColumn(
            ParserHelper.getLocation(ctx),
            (BaseExpression)visit(ctx.expression()),
            visitIfPresent(ctx.identifier(), Identifier.class).orElse(null),
            ctx.KW_AS() != null
        );
    }

    @Override
    public Node visitSelectAll(SelectAllContext ctx) {
        List<Identifier> aliases = ImmutableList.of();
        if (ctx.columnAliases() != null) {
            aliases = visit(ctx.columnAliases().identifier(), Identifier.class);
        }
        return new AllColumns(
            ParserHelper.getLocation(ctx),
            visitIfPresent(ctx.atomExpression(), BaseExpression.class).orElse(null),
            aliases, ctx.KW_AS() != null);
    }

    @Override
    public Node visitRelationDefault(RelationDefaultContext ctx) {
        return visit(ctx.sampledRelation());
    }

    @Override
    public Node visitSampledRelation(SampledRelationContext ctx) {
        BaseRelation child = (BaseRelation)visit(ctx.aliasedRelation());

        if (ctx.KW_TABLESAMPLE() == null) {
            return child;
        }

        return new SampledRelation(
            ParserHelper.getLocation(ctx),
            child,
            getSamplingMethod((Token)ctx.sampleType().getChild(0).getPayload()),
            (BaseExpression)visit(ctx.percentage));
    }

    @Override
    public Node visitAliasedRelation(AliasedRelationContext ctx) {
        BaseRelation child = (BaseRelation)visit(ctx.relationPrimary());

        if (ctx.identifierWithoutSql11() == null) {
            return child;
        }

        List<Identifier> aliases = null;
        if (ctx.columnAliases() != null) {
            aliases = visit(ctx.columnAliases().identifier(), Identifier.class);
        }

        return new AliasedRelation(ParserHelper.getLocation(ctx), child,
            (Identifier)visit(ctx.identifierWithoutSql11()), aliases);
    }

    @Override
    public Node visitJoinRelation(JoinRelationContext ctx) {
        BaseRelation left = (BaseRelation)visit(ctx.left);
        BaseRelation right;

        if (ctx.KW_CROSS() != null) {
            right = (BaseRelation)visit(ctx.right);
            return new Join(JoinToken.CROSS, left, right, null);
        }

        JoinCriteria criteria;
        if (ctx.KW_NATURAL() != null) {
            right = (BaseRelation)visit(ctx.right);
            criteria = new NaturalJoin();
        } else {
            right = (BaseRelation)visit(ctx.rightRelation);
            if (ctx.joinCriteria().KW_ON() != null) {
                criteria = new JoinOn((BaseExpression)visit(ctx.joinCriteria().expression()));
            } else if (ctx.joinCriteria().KW_USING() != null) {
                criteria = new JoinUsing(visit(ctx.joinCriteria().identifier(), Identifier.class));
            } else {
                throw new IllegalArgumentException("Unsupported join criteria");
            }
        }

        JoinToken joinToken = null;
        boolean isOuter = ctx.joinType().KW_OUTER() != null;
        if (ctx.joinType().KW_LEFT() != null) {
            if (isOuter) {
                joinToken = JoinToken.LEFT_OUTER;
            } else {
                joinToken = JoinToken.LEFT;
            }
        } else if (ctx.joinType().KW_RIGHT() != null) {
            if (isOuter) {
                joinToken = JoinToken.RIGHT_OUTER;
            } else {
                joinToken = JoinToken.RIGHT;
            }
        } else if (ctx.joinType().KW_FULL() != null) {
            if (isOuter) {
                joinToken = JoinToken.FULL_OUTER;
            } else {
                joinToken = JoinToken.FULL;
            }
        } else if (ctx.joinType().KW_INNER() != null) {
            joinToken = JoinToken.INNER;
        } else {
            joinToken = JoinToken.NULL;
        }
        return new Join(joinToken, left, right, criteria);
    }

    @Override
    public Node visitParenthesizedRelation(ParenthesizedRelationContext context) {
        return visit(context.relation());
    }

    @Override
    public Node visitSubqueryRelation(SubqueryRelationContext ctx) {
        return new TableSubQuery((Query)visit(ctx.query()));
    }

    @Override
    public Node visitGroupingOperation(GroupingOperationContext ctx) {
        List<QualifiedName> arguments = ctx.qualifiedName().stream()
            .map(this::getQualifiedName)
            .collect(toList());
        return new GroupingOperation(getLocation(ctx), getOrigin(ctx), arguments);
    }

    @Override
    public Node visitDelete(DeleteContext ctx) {
        DeleteType deleteType = DeleteType.TABLE;
        if (ctx.deleteType() != null) {
            deleteType = DeleteType.getByCode(ctx.deleteType().getText());
        }
        return new Delete(
            getQualifiedName(ctx.qualifiedName()),
            new WhereCondition((BaseExpression)visit(ctx.expression())),
            deleteType
        );
    }

    @Override
    public Node visitInsertInto(InsertIntoContext ctx) {
        List<Identifier> columns = ImmutableList.of();
        if (ctx.columnParenthesesList() != null) {
            columns = visit(ctx.columnParenthesesList().columnNameList().columnName(), Identifier.class);
        }
        List<PartitionSpec> partitionSpecList = ImmutableList.of();
        if (ctx.partitionSpec() != null) {
            partitionSpecList = visit(ctx.partitionSpec().partitionExpression(), PartitionSpec.class);
        }
        return new Insert(
            ctx.KW_OVERWRITE() != null, getQualifiedName(ctx.tableName()),
            partitionSpecList, (Query)visit(ctx.query()),
            columns
        );
    }

    private NullOrdering getNullOrderingType(Token token) {
        return NullOrdering.getByCode(token.getText());
    }

    private Ordering getOrderingType(Token token) {
        return Ordering.getByCode(token.getText());
    }

    private static boolean isDistinct(SetQuantifierContext setQuantifier) {
        return setQuantifier != null && setQuantifier.KW_DISTINCT() != null;
    }

    private static SampledType getSamplingMethod(Token token) {
        return SampledType.getByCode(token.getText());
    }

}
