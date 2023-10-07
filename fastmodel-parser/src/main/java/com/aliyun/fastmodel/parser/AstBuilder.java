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

package com.aliyun.fastmodel.parser;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

import com.aliyun.fastmodel.common.parser.ParserHelper;
import com.aliyun.fastmodel.common.utils.StripUtils;
import com.aliyun.fastmodel.core.exception.ParseException;
import com.aliyun.fastmodel.core.tree.AliasedName;
import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeParameter;
import com.aliyun.fastmodel.core.tree.datatype.Field;
import com.aliyun.fastmodel.core.tree.datatype.GenericDataType;
import com.aliyun.fastmodel.core.tree.datatype.JsonDataType;
import com.aliyun.fastmodel.core.tree.datatype.NumericParameter;
import com.aliyun.fastmodel.core.tree.datatype.RowDataType;
import com.aliyun.fastmodel.core.tree.datatype.TypeParameter;
import com.aliyun.fastmodel.core.tree.expr.ArithmeticBinaryExpression;
import com.aliyun.fastmodel.core.tree.expr.ArithmeticUnaryExpression;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.BitOperationExpression;
import com.aliyun.fastmodel.core.tree.expr.ComparisonExpression;
import com.aliyun.fastmodel.core.tree.expr.DereferenceExpression;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.IsConditionExpression;
import com.aliyun.fastmodel.core.tree.expr.LogicalBinaryExpression;
import com.aliyun.fastmodel.core.tree.expr.Row;
import com.aliyun.fastmodel.core.tree.expr.atom.Cast;
import com.aliyun.fastmodel.core.tree.expr.atom.CoalesceExpression;
import com.aliyun.fastmodel.core.tree.expr.atom.ExistsPredicate;
import com.aliyun.fastmodel.core.tree.expr.atom.Extract;
import com.aliyun.fastmodel.core.tree.expr.atom.Floor;
import com.aliyun.fastmodel.core.tree.expr.atom.FunctionCall;
import com.aliyun.fastmodel.core.tree.expr.atom.IfExpression;
import com.aliyun.fastmodel.core.tree.expr.atom.InPredicate;
import com.aliyun.fastmodel.core.tree.expr.atom.IntervalExpression;
import com.aliyun.fastmodel.core.tree.expr.atom.SearchedCaseExpression;
import com.aliyun.fastmodel.core.tree.expr.atom.SimpleCaseExpression;
import com.aliyun.fastmodel.core.tree.expr.atom.SubQueryExpression;
import com.aliyun.fastmodel.core.tree.expr.atom.TableOrColumn;
import com.aliyun.fastmodel.core.tree.expr.atom.WhenClause;
import com.aliyun.fastmodel.core.tree.expr.enums.ArithmeticOperator;
import com.aliyun.fastmodel.core.tree.expr.enums.BitOperator;
import com.aliyun.fastmodel.core.tree.expr.enums.ComparisonOperator;
import com.aliyun.fastmodel.core.tree.expr.enums.DateTimeEnum;
import com.aliyun.fastmodel.core.tree.expr.enums.FrameBoundType;
import com.aliyun.fastmodel.core.tree.expr.enums.IntervalQualifiers;
import com.aliyun.fastmodel.core.tree.expr.enums.IsType;
import com.aliyun.fastmodel.core.tree.expr.enums.LikeCondition;
import com.aliyun.fastmodel.core.tree.expr.enums.LikeOperator;
import com.aliyun.fastmodel.core.tree.expr.enums.LogicalOperator;
import com.aliyun.fastmodel.core.tree.expr.enums.NullTreatment;
import com.aliyun.fastmodel.core.tree.expr.enums.Sign;
import com.aliyun.fastmodel.core.tree.expr.enums.VarType;
import com.aliyun.fastmodel.core.tree.expr.enums.WindowFrameType;
import com.aliyun.fastmodel.core.tree.expr.literal.BaseLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.BooleanLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.CurrentDate;
import com.aliyun.fastmodel.core.tree.expr.literal.CurrentTimestamp;
import com.aliyun.fastmodel.core.tree.expr.literal.DateLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.DecimalLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.DoubleLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.IntervalLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.ListStringLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.LongLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.NullLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.StringLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.TimestampLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.TimestampLocalTzLiteral;
import com.aliyun.fastmodel.core.tree.expr.similar.BetweenPredicate;
import com.aliyun.fastmodel.core.tree.expr.similar.InListExpression;
import com.aliyun.fastmodel.core.tree.expr.similar.LikePredicate;
import com.aliyun.fastmodel.core.tree.expr.similar.NotExpression;
import com.aliyun.fastmodel.core.tree.expr.window.FrameBound;
import com.aliyun.fastmodel.core.tree.expr.window.Window;
import com.aliyun.fastmodel.core.tree.expr.window.WindowFrame;
import com.aliyun.fastmodel.core.tree.relation.Lateral;
import com.aliyun.fastmodel.core.tree.relation.querybody.Table;
import com.aliyun.fastmodel.core.tree.statement.CompositeStatement;
import com.aliyun.fastmodel.core.tree.statement.constants.ColumnCategory;
import com.aliyun.fastmodel.core.tree.statement.misc.EmptyStatement;
import com.aliyun.fastmodel.core.tree.statement.rule.PartitionSpec;
import com.aliyun.fastmodel.core.tree.statement.select.Query;
import com.aliyun.fastmodel.core.tree.statement.select.order.OrderBy;
import com.aliyun.fastmodel.core.tree.statement.select.order.SortItem;
import com.aliyun.fastmodel.core.tree.statement.showcreate.Output;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.DropCol;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.BaseConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.NotNullConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.PrimaryConstraint;
import com.aliyun.fastmodel.core.tree.util.IdentifierUtil;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.AliasContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.AlterColumnConstraintContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.AlterStatementSuffixDropConstraintContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.AlterStatementSuffixSetCommentContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.ArithmeticContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.AtomContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.BetweenPredictContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.BitOperationContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.BooleanValueContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.BoundedFrameContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.CaseExpressionContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.CastExpressionContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.ColTypeContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.ColonTypeContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.ColumnConstraintTypeContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.ColumnDefinitionContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.ColumnNameContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.ColumnNameTypeContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.ColumnParenthesesListContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.ColumnVarContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.CommentContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.ComparisonContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.ConstantContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.CurrentRowBoundContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.CustomTypeContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.DateLiteralContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.DecimalLiteralContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.DereferenceContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.DistinctFromContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.DoubleLiteralContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.DoublePrecisionTypeContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.DropColContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.EmptyStatementContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.EnableSpecificationContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.ExistsContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.ExpressionsInParenthesisContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.ExtractExpressionContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.FilterContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.FloorExpressionContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.FunctionCallContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.FunctionExpressionContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.GenericTypeContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.IdentifierContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.IdentifierWithoutSql11Context;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.InExpressionContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.InSubQueryContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.IntegerLiteralContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.IntervalExpressionContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.IntervalLiteralContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.IntervalValueContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.IsCondExpressionContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.IsConditionContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.JsonTypeContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.KeyPropertyContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.KeyValuePropertyContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.LateralContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.LikeExpressionContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.ListTypeContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.LogicalBinaryContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.MapTypeContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.NotExpressionContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.OutputContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.OverContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.ParenthesizedExpressionContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.PartitionExpressionContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.QualifiedNameContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.ReferenceColumnListContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.ReferencesObjectsContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.RootContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.RowConstructorContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.SetAliasedNameContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.SetPropertiesContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.ShowObjectTypesContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.ShowTypeContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.SqlStatementContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.SqlStatementsContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.StringContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.StringLiteralSequenceContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.StructTypeContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.SubqueryExpressionContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.SubstringContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.TableColumnContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.TableNameContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.TableNameListContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.TableQualifiedNameContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.TimestampLiteralContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.TimestampLocalTZLiteralContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.TypeParameterContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.UnAryExpressionContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.UnSetPropertiesContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.UnboundedFrameContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.WhenClauseContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.WhenExpressionContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.WindowFrameContext;
import com.aliyun.fastmodel.parser.generate.FastModelGrammarParserBaseVisitor;
import com.aliyun.fastmodel.parser.generate.FastModelLexer;
import com.google.common.collect.ImmutableList;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;

import static com.aliyun.fastmodel.common.parser.ParserHelper.getLocation;
import static com.aliyun.fastmodel.common.parser.ParserHelper.getOrigin;
import static java.util.stream.Collectors.toList;

/**
 * 核心的Builder类
 *
 * @author panguanjing
 * @date 2020/11/5
 */
public class AstBuilder extends FastModelGrammarParserBaseVisitor<Node> {

    public static final String IF = "if";
    public static final String COALESCE = "coalesce";
    public static final int IF_FUNCTION_LENGTH = 3;
    public static final int ELSE_EXP_INDEX = 2;
    public static final int START_DATE_INDEX = 2;

    @Override
    public Node visitRoot(RootContext ctx) {
        return visit(ctx.sqlStatements());
    }

    @Override
    public Node visitSqlStatements(SqlStatementsContext ctx) {
        List<SqlStatementContext> sqlStatementContexts = ctx.sqlStatement();
        List<BaseStatement> list = new ArrayList<>();
        if (sqlStatementContexts == null) {
            throw new ParseException("can't parse the empty context");
        }
        if (sqlStatementContexts.size() == 1) {
            return visit(sqlStatementContexts.get(0));
        }
        for (SqlStatementContext sqlStatementContext : sqlStatementContexts) {
            Node node = visit(sqlStatementContext);
            list.add((BaseStatement)node);
        }
        return new CompositeStatement(getLocation(ctx), getOrigin(ctx), list);
    }

    @Override
    public Node visitSqlStatement(SqlStatementContext ctx) {
        BaseStatement statement = (BaseStatement)super.visitSqlStatement(ctx);
        statement.setOrigin(getOrigin(ctx));
        return statement;
    }

    @Override
    public Node visitEmptyStatement(EmptyStatementContext ctx) {
        return new EmptyStatement();
    }

    @Override
    public Node visitExpressionsInParenthesis(ExpressionsInParenthesisContext ctx) {
        return new Row(getLocation(ctx), getOrigin(ctx), visit(ctx.expression(), BaseExpression.class));
    }

    @Override
    public Node visitAlias(AliasContext ctx) {
        StringLiteral visit = (StringLiteral)visit(ctx.string());
        return new AliasedName(visit.getValue());
    }

    @Override
    public Node visitSetAliasedName(SetAliasedNameContext ctx) {
        return visit(ctx.alias());
    }

    @Override
    public Node visitIdentifier(IdentifierContext ctx) {
        return getIdentifier(ctx);
    }

    protected Identifier getIdentifier(ParserRuleContext ctx) {
        return ParserHelper.getIdentifier(ctx);
    }

    @Override
    public Node visitIdentifierWithoutSql11(IdentifierWithoutSql11Context ctx) {
        return getIdentifier(ctx);
    }

    @Override
    public Node visitTableColumn(TableColumnContext ctx) {
        ColumnVarContext columnVarContext = ctx.columnVar();
        if (columnVarContext != null) {
            boolean isMacro = columnVarContext.MACRO() != null && !columnVarContext.MACRO().isEmpty();
            if (isMacro) {
                return new TableOrColumn(getLocation(ctx), getOrigin(ctx),
                    getQualifiedName(columnVarContext.tableName()), VarType.MACRO);
            }
            boolean isDollar = columnVarContext.DOLLAR() != null;
            if (isDollar) {
                return new TableOrColumn(getLocation(ctx), getOrigin(ctx),
                    getQualifiedName(columnVarContext.tableName()), VarType.DOLLAR);
            }
        }
        return new TableOrColumn(getLocation(ctx), getOrigin(ctx), getQualifiedName(ctx.tableName()), null);
    }

    @Override
    public Node visitOver(OverContext ctx) {
        OrderBy orderBy = null;
        if (ctx.KW_ORDER() != null) {
            orderBy = new OrderBy(getLocation(ctx.KW_ORDER()), visit(ctx.sortItem(), SortItem.class));
        }

        return new Window(
            getLocation(ctx),
            visit(ctx.partition, BaseExpression.class),
            orderBy,
            visitIfPresent(ctx.windowFrame(), WindowFrame.class).orElse(null));
    }

    @Override
    public Node visitWindowFrame(WindowFrameContext ctx) {
        Token frameType = ctx.frameType;
        return new WindowFrame(
            getLocation(ctx),
            getFrameType(frameType),
            (FrameBound)visit(ctx.start),
            visitIfPresent(ctx.end, FrameBound.class).orElse(null)
        );
    }

    @Override
    public Node visitUnboundedFrame(UnboundedFrameContext ctx) {
        return new FrameBound(getLocation(ctx), getUnboundedFrameBoundType(ctx.boundType));
    }

    @Override
    public Node visitCurrentRowBound(CurrentRowBoundContext ctx) {
        return new FrameBound(getLocation(ctx), FrameBoundType.CURRENT_ROW);
    }

    @Override
    public Node visitBoundedFrame(BoundedFrameContext ctx) {
        return new FrameBound(getLocation(ctx), getBoundedFrameBoundType(ctx.boundType),
            (BaseExpression)visit(ctx.expression()));
    }

    @Override
    public Node visitFilter(FilterContext ctx) {
        return visit(ctx.expression());
    }

    @Override
    public Node visitFunctionExpression(FunctionExpressionContext ctx) {
        QualifiedName name = getQualifiedName(ctx.qualifiedName());

        if (name.toString().equalsIgnoreCase(IF)) {
            BaseExpression elseExpression = null;
            if (ctx.expression().size() == IF_FUNCTION_LENGTH) {
                elseExpression = (BaseExpression)visit(ctx.expression(ELSE_EXP_INDEX));
            }
            return new IfExpression(
                getLocation(ctx),
                getOrigin(ctx),
                (BaseExpression)visit(ctx.expression(0)),
                (BaseExpression)visit(ctx.expression(1)),
                elseExpression);
        }

        if (name.toString().equalsIgnoreCase(COALESCE)) {
            return new CoalesceExpression(getLocation(ctx), getOrigin(ctx),
                visit(ctx.expression(), BaseExpression.class));
        }

        List<BaseExpression> list = visit(ctx.expression(), BaseExpression.class);
        OrderBy orderBy = null;
        if (ctx.KW_ORDER() != null) {
            orderBy = new OrderBy(visit(ctx.sortItem(), SortItem.class));
        }
        Optional<BaseExpression> baseExpression = visitIfPresent(ctx.filter(), BaseExpression.class);
        Optional<Window> window = visitIfPresent(ctx.over(), Window.class);
        NullTreatment nulls = null;
        if (ctx.nullTreatment() != null) {
            if (ctx.nullTreatment().KW_IGNORE() != null) {
                nulls = NullTreatment.IGNORE;
            } else if (ctx.nullTreatment().KW_RESPECT() != null) {
                nulls = NullTreatment.RESPECT;
            }
        }
        boolean distinct = ctx.setQuantifier() != null && ctx.setQuantifier().KW_DISTINCT() != null;
        return new FunctionCall(
            getLocation(ctx),
            getOrigin(ctx),
            getQualifiedName(ctx.qualifiedName()),
            distinct,
            list,
            window.orElse(null), nulls, baseExpression.orElse(null), orderBy);
    }

    @Override
    public Node visitFunctionCall(FunctionCallContext functionCallContext) {
        return visit(functionCallContext.functionExpression());
    }

    @Override
    public Node visitCastExpression(CastExpressionContext ctx) {
        return new Cast(
            getLocation(ctx),
            getOrigin(ctx),
            (BaseExpression)visit(ctx.expression()),
            (BaseDataType)visit(ctx.primitiveType())
        );
    }

    @Override
    public Node visitOutput(OutputContext ctx) {
        Identifier visit = (Identifier)visit(ctx.identifier());
        return new Output(visit);
    }

    @Override
    public Node visitShowType(ShowTypeContext ctx) {
        return new Identifier(ctx.getText());
    }

    @Override
    public Node visitShowObjectTypes(ShowObjectTypesContext ctx) {
        return new Identifier(ctx.getText());
    }

    @Override
    public Node visitCaseExpression(CaseExpressionContext ctx) {
        return new SimpleCaseExpression(
            getLocation(ctx),
            getOrigin(ctx),
            (BaseExpression)visit(ctx.operand),
            visit(ctx.whenClause(), WhenClause.class),
            visitIfPresent(ctx.elseExpression, BaseExpression.class).orElse(null)
        );
    }

    @Override
    public Node visitWhenExpression(WhenExpressionContext ctx) {
        return new SearchedCaseExpression(
            getLocation(ctx),
            getOrigin(ctx),
            visit(ctx.whenClause(), WhenClause.class),
            visitIfPresent(ctx.elseExpression, BaseExpression.class).orElse(null)
        );
    }

    @Override
    public Node visitFloorExpression(FloorExpressionContext ctx) {
        return new Floor(
            getLocation(ctx),
            getOrigin(ctx),
            (BaseExpression)visit(ctx.expression()),
            DateTimeEnum.getByCode(ctx.floorDateQualifiers().getText())
        );
    }

    @Override
    public Node visitExtractExpression(ExtractExpressionContext ctx) {
        return new Extract(
            getLocation(ctx),
            getOrigin(ctx),
            DateTimeEnum.getByCode(ctx.timeQualifiers().getText()),
            (BaseExpression)visit(ctx.expression())
        );
    }

    @Override
    public Node visitComparison(ComparisonContext ctx) {
        return new ComparisonExpression(
            getLocation(ctx),
            getOrigin(ctx),
            ComparisonOperator.getByCode(ctx.comparisonOperator().getText()),
            (BaseExpression)visit(ctx.left),
            (BaseExpression)visit(ctx.right)
        );
    }

    @Override
    public Node visitSubqueryExpression(SubqueryExpressionContext ctx) {
        return new SubQueryExpression(
            getLocation(ctx),
            getOrigin(ctx),
            (Query)visit(ctx.query()));
    }

    @Override
    public Node visitExists(ExistsContext ctx) {
        return new ExistsPredicate(
            getLocation(ctx),
            getOrigin(ctx),
            new SubQueryExpression(getLocation(ctx.query()), getOrigin(ctx.query()), (Query)visit(ctx.query()))
        );
    }

    @Override
    public Node visitDereference(DereferenceContext ctx) {
        return new DereferenceExpression(
            getLocation(ctx),
            getOrigin(ctx),
            (BaseExpression)visit(ctx.base),
            (Identifier)visit(ctx.fieldName));
    }

    @Override
    public Node visitRowConstructor(RowConstructorContext ctx) {
        return new Row(getLocation(ctx), getOrigin(ctx), visit(ctx.expression(), BaseExpression.class));
    }

    @Override
    public Node visitDistinctFrom(DistinctFromContext ctx) {
        TerminalNode terminalNode = ctx.KW_NOT();
        BaseExpression expression = new ComparisonExpression(
            ComparisonOperator.IS_DISTINCT_FROM,
            (BaseExpression)visit(ctx.left),
            (BaseExpression)visit(ctx.right));
        if (terminalNode != null) {
            expression = new NotExpression(getLocation(ctx), getOrigin(ctx), expression);
        }
        return expression;
    }

    @Override
    public Node visitArithmetic(ArithmeticContext ctx) {
        return
            new ArithmeticBinaryExpression(
                getLocation(ctx),
                getOrigin(ctx),
                ArithmeticOperator.getByCode(ctx.operator.getText()),
                (BaseExpression)visit(ctx.left),
                (BaseExpression)visit(ctx.right)
            );
    }

    @Override
    public Node visitWhenClause(WhenClauseContext ctx) {
        return new WhenClause(
            getLocation(ctx),
            getOrigin(ctx),
            (BaseExpression)visit(ctx.when),
            (BaseExpression)visit(ctx.then)
        );
    }

    @Override
    public Node visitNotExpression(NotExpressionContext ctx) {
        return new NotExpression(getLocation(ctx), getOrigin(ctx), (BaseExpression)visit(ctx.expression()));
    }

    @Override
    public Node visitBetweenPredict(BetweenPredictContext ctx) {
        BaseExpression baseExpression = new BetweenPredicate(
            getLocation(ctx),
            getOrigin(ctx),
            (BaseExpression)visit(ctx.left),
            (BaseExpression)visit(ctx.lower),
            (BaseExpression)visit(ctx.upper)
        );
        if (ctx.KW_NOT() != null) {
            return new NotExpression(getLocation(ctx), getOrigin(ctx), baseExpression);
        }
        return baseExpression;
    }

    @Override
    public Node visitColumnDefinition(ColumnDefinitionContext ctx) {
        Boolean primary = null;
        //不设置的话，默认就是false
        Boolean notNull = null;
        if (ctx.columnConstraintType() != null) {
            List<ColumnConstraintTypeContext> columnConstraintTypeContexts = ctx.columnConstraintType();
            for (ColumnConstraintTypeContext c : columnConstraintTypeContexts) {
                if (c.KW_PRIMARY() != null) {
                    //如果是主键，那么notNull同时不为空
                    primary = true;
                    notNull = true;
                }
                if (c.KW_NOT() != null) {
                    notNull = true;
                }
            }
        }
        BaseLiteral baseLiteral = visitIfPresent(ctx.defaultValue(), BaseLiteral.class).orElse(null);
        ColumnCategory category = ColumnCategory.ATTRIBUTE;
        if (ctx.category() != null) {
            category = ColumnCategory.getByCode(ctx.category().getText());
        }
        List<Property> properties = ImmutableList.of();
        if (ctx.tableProperties() != null) {
            properties = visit(ctx.tableProperties().keyValueProperty(), Property.class);
        }
        AliasedName aliasedName = visitIfPresent(ctx.alias(), AliasedName.class).orElse(null);
        ReferencesObjectsContext referencesObjectsContext = ctx.referencesObjects();
        List<Identifier> indicators = ImmutableList.of();
        QualifiedName refDimensions = null;
        if (referencesObjectsContext != null) {
            ReferenceColumnListContext referenceColumnListContext = referencesObjectsContext.referenceColumnList();
            if (referenceColumnListContext != null) {
                indicators =
                    visit(referenceColumnListContext.columnParenthesesList().columnNameList().columnName(),
                        Identifier.class);
            } else {
                refDimensions = getQualifiedName(referencesObjectsContext.qualifiedName());
            }
        }
        Comment comment = ctx.comment() != null ? (Comment)visit(ctx.comment()) : null;
        return ColumnDefinition.builder().colName(
                (Identifier)visit(ctx.identifier()))
            .aliasedName(aliasedName).dataType((BaseDataType)visit(ctx.colType()))
            .comment(comment)
            .primary(primary)
            .notNull(notNull)
            .defaultValue(baseLiteral)
            .category(category)
            .properties(properties)
            .refDimension(refDimensions)
            .refIndicators(indicators)
            .build();

    }

    @Override
    public Node visitColumnConstraintType(ColumnConstraintTypeContext ctx) {
        if (ctx.KW_NOT() != null) {
            return new NotNullConstraint(IdentifierUtil.sysIdentifier(), true);
        } else if (ctx.KW_NULL() != null) {
            return new NotNullConstraint(IdentifierUtil.sysIdentifier(), false);
        }
        if (ctx.KW_PRIMARY() != null) {
            return new PrimaryConstraint(IdentifierUtil.sysIdentifier(), ImmutableList.of(), true);
        }
        return null;
    }

    @Override
    public Node visitColumnParenthesesList(ColumnParenthesesListContext ctx) {
        return visit(ctx.columnNameList());
    }

    @Override
    public Node visitColumnNameType(ColumnNameTypeContext ctx) {
        return visit(ctx.columnDefinition());
    }

    @Override
    public Node visitColumnName(ColumnNameContext ctx) {
        return visit(ctx.identifier());
    }

    @Override
    public Node visitColType(ColTypeContext ctx) {
        return visit(ctx.typeDbCol());
    }

    @Override
    public Node visitGenericType(GenericTypeContext ctx) {
        List<DataTypeParameter> parameters = ctx.typeParameter().stream()
            .map(this::visit)
            .map(DataTypeParameter.class::cast)
            .collect(toList());

        return new GenericDataType(getLocation(ctx), getOrigin(ctx), ctx.name.getText(), parameters);
    }

    @Override
    public Node visitCustomType(CustomTypeContext ctx) {
        List<DataTypeParameter> parameters = ctx.typeParameter().stream()
            .map(this::visit)
            .map(DataTypeParameter.class::cast)
            .collect(toList());
        StringLiteral stringLiteral = (StringLiteral)visit(ctx.string());
        return new GenericDataType(getLocation(ctx), getOrigin(ctx), stringLiteral.getValue(),
            parameters);
    }

    @Override
    public Node visitTypeParameter(TypeParameterContext ctx) {
        TerminalNode terminalNode = ctx.INTEGER_VALUE();
        if (terminalNode != null) {
            return new NumericParameter(terminalNode.getText());
        } else {
            return new TypeParameter((BaseDataType)visit(ctx.typeDbCol()));
        }
    }

    @Override
    public Node visitListType(ListTypeContext ctx) {
        String text = ctx.KW_ARRAY().getText();
        return new GenericDataType(
            getLocation(ctx),
            getOrigin(ctx),
            text,
            ImmutableList.of(new TypeParameter((BaseDataType)visit(ctx.typeDbCol()))));
    }

    @Override
    public Node visitDoublePrecisionType(DoublePrecisionTypeContext ctx) {
        String text = ctx.KW_DOUBLE().getText();
        return new GenericDataType(
            getLocation(ctx),
            getOrigin(ctx),
            text,
            ImmutableList.of());
    }

    @Override
    public Node visitStructType(StructTypeContext ctx) {
        List<Field> list = visit(ctx.typeList().colonType(), Field.class);
        return new RowDataType(list);
    }

    @Override
    public Node visitJsonType(JsonTypeContext ctx) {
        if (ctx.typeList() != null) {
            List<Field> list = visit(ctx.typeList().colonType(), Field.class);
            return new JsonDataType(getLocation(ctx), getOrigin(ctx), list);
        }
        return new JsonDataType(getLocation(ctx), getOrigin(ctx), null);
    }

    @Override
    public Node visitColonType(ColonTypeContext ctx) {
        Identifier identifier = (Identifier)visit(ctx.identifier());
        BaseDataType baseDataType = (BaseDataType)visit(ctx.typeDbCol());
        Comment comment = visitIfPresent(ctx.comment(), Comment.class).orElse(null);
        return new Field(identifier, baseDataType, comment);
    }

    @Override
    public Node visitMapType(MapTypeContext ctx) {
        String text = ctx.KW_MAP().getText();
        return new GenericDataType(
            getLocation(ctx),
            getOrigin(ctx),
            text,
            ImmutableList.of(
                new TypeParameter((BaseDataType)visit(ctx.key)),
                new TypeParameter((BaseDataType)visit(ctx.value))));
    }

    @Override
    public Node visitDropCol(DropColContext ctx) {
        return new DropCol(
            getQualifiedName(ctx.tableName()),
            (Identifier)visit(ctx.columnName())
        );
    }

    protected Boolean isNotNull(BaseConstraint baseConstraint) {
        if (baseConstraint == null) {
            return null;
        }
        boolean notNull = baseConstraint instanceof NotNullConstraint;
        if (!notNull) {
            return null;
        }
        NotNullConstraint notNullConstraint = (NotNullConstraint)baseConstraint;
        return notNullConstraint.getEnable();
    }

    protected Boolean isPrimary(BaseConstraint baseConstraint) {
        if (baseConstraint == null) {
            return null;
        }
        boolean primary = baseConstraint instanceof PrimaryConstraint;
        if (!primary) {
            return null;
        }
        PrimaryConstraint primaryConstraint = (PrimaryConstraint)baseConstraint;
        return primaryConstraint.getEnable();
    }

    @Override
    public Node visitAlterColumnConstraint(AlterColumnConstraintContext ctx) {
        Identifier colName = (Identifier)visit(ctx.value);
        ColumnConstraintTypeContext columnConstraintTypeContext = ctx.columnConstraintType();
        EnableSpecificationContext enableSpecificationContext = ctx.enableSpecification();
        boolean enable = enableSpecificationContext == null || enableSpecificationContext.KW_ENABLE() != null;
        if (columnConstraintTypeContext.KW_NOT() != null) {
            return new NotNullConstraint(IdentifierUtil.sysIdentifier(), enable);
        }
        if (columnConstraintTypeContext.KW_PRIMARY() != null) {
            return new PrimaryConstraint(
                IdentifierUtil.sysIdentifier(), ImmutableList.of(colName), enable);
        }
        return super.visitAlterColumnConstraint(ctx);
    }

    @Override
    public Node visitInExpression(InExpressionContext ctx) {
        BaseExpression expression = new InListExpression(
            getLocation(ctx),
            getOrigin(ctx),
            (BaseExpression)visit(ctx.left),
            visit(ctx.expressionsInParenthesis().expression(), BaseExpression.class)
        );
        TerminalNode terminalNode = ctx.KW_NOT();
        if (terminalNode != null) {
            expression = new NotExpression(getLocation(ctx), getOrigin(ctx), expression);
        }
        return expression;
    }

    @Override
    public Node visitBitOperation(BitOperationContext ctx) {
        return new BitOperationExpression(
            getLocation(ctx),
            getOrigin(ctx),
            BitOperator.getByCode(ctx.operator.getText()),
            (BaseExpression)visit(ctx.left),
            (BaseExpression)visit(ctx.right)
        );
    }

    @Override
    public Node visitLikeExpression(LikeExpressionContext ctx) {
        LikeCondition likeCondition = null;
        LikeOperator likeOperator = null;
        if (ctx.operator != null) {
            likeOperator = LikeOperator.valueOf(ctx.operator.getText().toUpperCase(Locale.ROOT));
        }
        if (ctx.condition != null) {
            likeCondition = LikeCondition.getByCode(ctx.condition.getText());
        }
        BaseExpression likePredicate = new LikePredicate(
            getLocation(ctx),
            getOrigin(ctx),
            (BaseExpression)visit(ctx.left),
            likeOperator,
            likeCondition,
            (BaseExpression)visit(ctx.right)
        );
        TerminalNode terminalNode = ctx.KW_NOT();
        if (terminalNode != null) {
            likePredicate = new NotExpression(getLocation(ctx), getOrigin(ctx), likePredicate);
        }
        return likePredicate;
    }

    @Override
    public Node visitAtom(AtomContext ctx) {
        return visit(ctx.atomExpression());
    }

    @Override
    public Node visitIsCondExpression(IsCondExpressionContext ctx) {
        return new IsConditionExpression(
            getLocation(ctx),
            getOrigin(ctx),
            (BaseExpression)visit(ctx.expression()),
            getIsType(ctx.isCondition())
        );
    }

    private IsType getIsType(IsConditionContext condition) {
        TerminalNode kwNull = condition.KW_NULL();
        TerminalNode kwNot = condition.KW_NOT();
        TerminalNode kwTrue = condition.KW_TRUE();
        TerminalNode kwFalse = condition.KW_FALSE();
        if (kwNull != null) {
            if (kwNot != null) {
                return IsType.NOT_NULL;
            } else {
                return IsType.NULL;
            }
        }
        if (kwTrue != null) {
            if (kwNot != null) {
                return IsType.NOT_TRUE;
            } else {
                return IsType.TRUE;
            }
        }
        if (kwFalse != null) {
            if (kwNot != null) {
                return IsType.NOT_FALSE;
            } else {
                return IsType.FALSE;
            }
        }
        throw new IllegalArgumentException("can't match the isCondition:" + getOrigin(condition));
    }

    @Override
    public Node visitUnAryExpression(UnAryExpressionContext ctx) {
        return new ArithmeticUnaryExpression(ParserHelper.getLocation(ctx), ParserHelper.getOrigin(ctx),
            Sign.getByCode(ctx.operator.getText()),
            (BaseExpression)visit(ctx.expression()));
    }

    @Override
    public Node visitLogicalBinary(LogicalBinaryContext ctx) {
        return new LogicalBinaryExpression(
            getLocation(ctx),
            getOrigin(ctx),
            LogicalOperator.getByCode(ctx.operator.getText()),
            (BaseExpression)visit(ctx.left),
            (BaseExpression)visit(ctx.right)
        );
    }

    @Override
    public Node visitInSubQuery(InSubQueryContext ctx) {
        BaseExpression result = new InPredicate(
            getLocation(ctx),
            getOrigin(ctx),
            (BaseExpression)visit(ctx.left),
            new SubQueryExpression(getLocation(ctx), getOrigin(ctx), (Query)visit(ctx.query())));

        if (ctx.KW_NOT() != null) {
            result = new NotExpression(getLocation(ctx), getOrigin(ctx), result);
        }
        return result;
    }

    @Override
    public Node visitSubstring(SubstringContext ctx) {
        return new FunctionCall(getLocation(ctx), getOrigin(ctx), QualifiedName.of("substr"),
            false,
            visit(ctx.expression(), BaseExpression.class));
    }

    @Override
    public Node visitParenthesizedExpression(ParenthesizedExpressionContext ctx) {
        BaseExpression visit = (BaseExpression)visit(ctx.expression());
        visit.setParenthesized(true);
        return visit;
    }

    @Override
    public Node visitIntervalExpression(IntervalExpressionContext ctx) {
        return new IntervalExpression(
            getLocation(ctx),
            getOrigin(ctx),
            (BaseLiteral)visit(ctx.intervalValue()),
            IntervalQualifiers.getIntervalQualifiers(ctx.intervalQualifiers().getText()),
            visitIfPresent(ctx.expression(), BaseExpression.class).orElse(null),
            ctx.KW_INTERVAL() != null
        );
    }

    private static WindowFrameType getFrameType(Token type) {
        switch (type.getType()) {
            case FastModelLexer.KW_RANGE:
                return WindowFrameType.RANGE;
            case FastModelLexer.KW_ROWS:
                return WindowFrameType.ROWS;
            case FastModelLexer.KW_GROUPS:
                return WindowFrameType.GROUPS;
            default:
                break;
        }

        throw new IllegalArgumentException("Unsupported frame type: " + type.getText());
    }

    private static FrameBoundType getBoundedFrameBoundType(Token token) {
        switch (token.getType()) {
            case FastModelLexer.KW_PRECEDING:
                return FrameBoundType.PRECEDING;
            case FastModelLexer.KW_FOLLOWING:
                return FrameBoundType.FOLLOWING;
            default:
                throw new IllegalArgumentException("Unsupported bound type: " + token.getText());
        }
    }

    private static FrameBoundType getUnboundedFrameBoundType(Token token) {
        switch (token.getType()) {
            case FastModelLexer.KW_PRECEDING:
                return FrameBoundType.UNBOUNDED_PRECEDING;
            case FastModelLexer.KW_FOLLOWING:
                return FrameBoundType.UNBOUNDED_FOLLOWING;
            default:
                throw new IllegalArgumentException("Unsupported bound type: " + token.getText());
        }
    }

    protected List<Property> getProperties(SetPropertiesContext context) {
        List<Property> properties = ImmutableList.of();
        if (context != null) {
            properties = visit(context.tableProperties().keyValueProperty(), Property.class);
        }
        return properties;
    }

    protected List<QualifiedName> getQualifiedName(TableNameListContext tableNameList) {
        List<QualifiedName> list = new ArrayList<>();
        for (TableNameContext t : tableNameList.tableName()) {
            list.add(getQualifiedName(t));
        }
        return list;
    }

    @Override
    public Node visitComment(CommentContext ctx) {
        StringLiteral stringLiteral = (StringLiteral)visit(ctx.string());
        return new Comment(stringLiteral.getValue());
    }

    @Override
    public Node visitKeyValueProperty(KeyValuePropertyContext ctx) {
        List<StringLiteral> list = visit(ctx.string(), StringLiteral.class);
        return new Property(
            list.get(0).getValue(),
            list.get(1).getValue()
        );
    }

    @Override
    public Node visitKeyProperty(KeyPropertyContext ctx) {
        StringLiteral list = (StringLiteral)visit(ctx.string());
        return new Identifier(list.getValue());
    }

    protected List<Identifier> getUnSetProperties(UnSetPropertiesContext unSetProperties) {
        return visit(unSetProperties.keyProperty(), Identifier.class);
    }

    protected List<String> getUnSetProopertyKeys(UnSetPropertiesContext unSetProperties) {
        return visit(unSetProperties.keyProperty(), Identifier.class).stream().map(Identifier::getValue).collect(
            toList());
    }

    @Override
    public Node visitAlterStatementSuffixSetComment(AlterStatementSuffixSetCommentContext ctx) {
        return visit(ctx.comment());
    }

    @Override
    public Node visitAlterStatementSuffixDropConstraint(AlterStatementSuffixDropConstraintContext ctx) {
        return visit(ctx.identifier());
    }

    @Override
    public Node visitConstant(ConstantContext ctx) {
        BooleanValueContext booleanValueContext = ctx.booleanValue();
        if (booleanValueContext != null) {
            return visit(booleanValueContext);
        }
        if (ctx.timestampLiteral() != null) {
            return visit(ctx.timestampLiteral());
        }
        if (ctx.timestampLocalTZLiteral() != null) {
            return visit(ctx.timestampLocalTZLiteral());
        }
        if (ctx.dateLiteral() != null) {
            return visit(ctx.dateLiteral());
        }
        if (ctx.intervalLiteral() != null) {
            return visit(ctx.intervalLiteral());
        }
        if (ctx.KW_NULL() != null) {
            return new NullLiteral(getLocation(ctx), getOrigin(ctx));
        }
        if (ctx.numberLiteral() != null) {
            return visit(ctx.numberLiteral());
        }
        if (ctx.string() != null) {
            return visit(ctx.string());
        }
        return super.visitConstant(ctx);
    }

    @Override
    public Node visitDecimalLiteral(DecimalLiteralContext ctx) {
        return new DecimalLiteral(ctx.getText());
    }

    /**
     * {@inheritDoc}
     *
     * <p>The default implementation returns the result of calling
     * {@link #visitChildren} on {@code ctx}.</p>
     */
    @Override
    public Node visitDoubleLiteral(DoubleLiteralContext ctx) {
        return new DoubleLiteral(getLocation(ctx), getOrigin(ctx), ctx.getText());
    }

    /**
     * {@inheritDoc}
     *
     * <p>The default implementation returns the result of calling
     * {@link #visitChildren} on {@code ctx}.</p>
     */
    @Override
    public Node visitIntegerLiteral(IntegerLiteralContext ctx) {
        return new LongLiteral(getLocation(ctx), getOrigin(ctx), ctx.getText());
    }

    @Override
    public Node visitStringLiteralSequence(StringLiteralSequenceContext ctx) {
        return new ListStringLiteral(
            getLocation(ctx),
            getOrigin(ctx),
            visit(ctx.string(), StringLiteral.class)
        );
    }

    @Override
    public Node visitDateLiteral(DateLiteralContext ctx) {
        if (ctx.KW_CURRENT_DATE() != null) {
            return new CurrentDate(getLocation(ctx), getOrigin(ctx));
        }
        StringLiteral stringLiteral = (StringLiteral)visit(ctx.string());
        return new DateLiteral(getLocation(ctx), getOrigin(ctx), stringLiteral.getValue());
    }

    @Override
    public Node visitTimestampLiteral(TimestampLiteralContext ctx) {
        TerminalNode terminalNode = ctx.KW_CURRENT_TIMESTAMP();
        if (terminalNode != null) {
            return new CurrentTimestamp(getLocation(ctx), getOrigin(ctx));
        }
        StringLiteral stringLiteral = (StringLiteral)visit(ctx.string());
        return new TimestampLiteral(getLocation(ctx), getOrigin(ctx), stringLiteral.getValue());
    }

    @Override
    public Node visitTimestampLocalTZLiteral(TimestampLocalTZLiteralContext ctx) {
        StringLiteral stringLiteral = (StringLiteral)visit(ctx.string());
        return new TimestampLocalTzLiteral(getLocation(ctx), getOrigin(ctx), stringLiteral.getValue());
    }

    @Override
    public Node visitIntervalValue(IntervalValueContext ctx) {
        StringContext string = ctx.string();
        if (string != null) {
            return visit(string);
        }
        if (ctx.numberLiteral() != null) {
            return visit(ctx.numberLiteral());
        }
        return super.visitIntervalValue(ctx);
    }

    @Override
    public Node visitIntervalLiteral(IntervalLiteralContext ctx) {
        IntervalValueContext intervalValueContext = ctx.intervalValue();
        return new IntervalLiteral(
            getLocation(ctx),
            getOrigin(ctx),
            (BaseLiteral)visit(intervalValueContext),
            DateTimeEnum.getByCode(ctx.getText()),
            null
        );
    }

    @Override
    public Node visitString(StringContext ctx) {
        String text = ctx.getText();
        return new StringLiteral(getLocation(ctx), getOrigin(ctx), StripUtils.strip(text));
    }

    @Override
    public Node visitBooleanValue(BooleanValueContext ctx) {
        return new BooleanLiteral(ctx.getText());
    }

    @Override
    public Node visitLateral(LateralContext ctx) {
        return new Lateral(ParserHelper.getLocation(ctx), (Query)visit(ctx.query()));
    }

    @Override
    public Node visitTableQualifiedName(TableQualifiedNameContext ctx) {
        return new Table(getQualifiedName(ctx.qualifiedName()));
    }

    @Override
    public Node visitPartitionExpression(PartitionExpressionContext ctx) {
        Identifier col = (Identifier)visit(ctx.columnName());
        BaseLiteral baseLiteral = (BaseLiteral)visit(ctx.constant());
        return new PartitionSpec(col, baseLiteral);
    }

    // ******** Helper ************

    protected <T> Optional<T> visitIfPresent(ParserRuleContext context, Class<T> clazz) {
        return ParserHelper.visitIfPresent(this, context, clazz);
    }

    protected <T> List<T> visit(List<? extends ParseTree> contexts, Class<T> clazz) {
        return ParserHelper.visit(this, contexts, clazz);
    }

    protected QualifiedName getQualifiedName(QualifiedNameContext context) {
        return QualifiedName.of(visit(context.identifier(), Identifier.class));
    }

    protected QualifiedName getQualifiedName(TableNameContext context) {
        return QualifiedName.of(visit(context.identifier(), Identifier.class));
    }

    protected AliasedName getAlias(AliasContext context) {
        return visitIfPresent(context, AliasedName.class).orElse(null);
    }

    protected Comment getComment(CommentContext context) {
        return visitIfPresent(context, Comment.class).orElse(null);
    }

}
