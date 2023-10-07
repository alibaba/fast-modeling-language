/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.clickhouse.parser.visitor;

import java.util.List;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.common.parser.ParserHelper;
import com.aliyun.fastmodel.common.utils.StripUtils;
import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.ListNode;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeParameter;
import com.aliyun.fastmodel.core.tree.datatype.TypeParameter;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.atom.FunctionCall;
import com.aliyun.fastmodel.core.tree.expr.atom.TableOrColumn;
import com.aliyun.fastmodel.core.tree.expr.literal.BaseLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.DoubleLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.HexLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.LongLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.NullLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.StringLiteral;
import com.aliyun.fastmodel.core.tree.statement.element.CreateElement;
import com.aliyun.fastmodel.core.tree.statement.select.item.AllColumns;
import com.aliyun.fastmodel.core.tree.statement.table.CloneTable;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.TableElement;
import com.aliyun.fastmodel.transform.api.context.ReverseContext;
import com.aliyun.fastmodel.transform.clickhouse.parser.ClickHouseParser.ColumnExprListContext;
import com.aliyun.fastmodel.transform.clickhouse.parser.ClickHouseParser.ColumnExprLiteralContext;
import com.aliyun.fastmodel.transform.clickhouse.parser.ClickHouseParser.ColumnTypeExprComplexContext;
import com.aliyun.fastmodel.transform.clickhouse.parser.ClickHouseParser.ColumnTypeExprEnumContext;
import com.aliyun.fastmodel.transform.clickhouse.parser.ClickHouseParser.ColumnTypeExprNestedContext;
import com.aliyun.fastmodel.transform.clickhouse.parser.ClickHouseParser.ColumnTypeExprParamContext;
import com.aliyun.fastmodel.transform.clickhouse.parser.ClickHouseParser.ColumnTypeExprSimpleContext;
import com.aliyun.fastmodel.transform.clickhouse.parser.ClickHouseParser.ColumnsExprAsteriskContext;
import com.aliyun.fastmodel.transform.clickhouse.parser.ClickHouseParser.ColumnsExprColumnContext;
import com.aliyun.fastmodel.transform.clickhouse.parser.ClickHouseParser.CreateTableStmtContext;
import com.aliyun.fastmodel.transform.clickhouse.parser.ClickHouseParser.DatabaseIdentifierContext;
import com.aliyun.fastmodel.transform.clickhouse.parser.ClickHouseParser.EnumValueContext;
import com.aliyun.fastmodel.transform.clickhouse.parser.ClickHouseParser.IdentifierContext;
import com.aliyun.fastmodel.transform.clickhouse.parser.ClickHouseParser.LiteralContext;
import com.aliyun.fastmodel.transform.clickhouse.parser.ClickHouseParser.NestedIdentifierContext;
import com.aliyun.fastmodel.transform.clickhouse.parser.ClickHouseParser.QueryStmtContext;
import com.aliyun.fastmodel.transform.clickhouse.parser.ClickHouseParser.SchemaAsFunctionClauseContext;
import com.aliyun.fastmodel.transform.clickhouse.parser.ClickHouseParser.SchemaAsTableClauseContext;
import com.aliyun.fastmodel.transform.clickhouse.parser.ClickHouseParser.SchemaDescriptionClauseContext;
import com.aliyun.fastmodel.transform.clickhouse.parser.ClickHouseParser.TableArgListContext;
import com.aliyun.fastmodel.transform.clickhouse.parser.ClickHouseParser.TableColumnDfntContext;
import com.aliyun.fastmodel.transform.clickhouse.parser.ClickHouseParser.TableElementExprColumnContext;
import com.aliyun.fastmodel.transform.clickhouse.parser.ClickHouseParser.TableElementExprConstraintContext;
import com.aliyun.fastmodel.transform.clickhouse.parser.ClickHouseParser.TableElementExprIndexContext;
import com.aliyun.fastmodel.transform.clickhouse.parser.ClickHouseParser.TableFunctionExprContext;
import com.aliyun.fastmodel.transform.clickhouse.parser.ClickHouseParser.TableIdentifierContext;
import com.aliyun.fastmodel.transform.clickhouse.parser.ClickHouseParser.TableSchemaClauseContext;
import com.aliyun.fastmodel.transform.clickhouse.parser.ClickHouseParserBaseVisitor;
import com.aliyun.fastmodel.transform.clickhouse.parser.tree.ClickHouseEnumDataTypeParameter;
import com.aliyun.fastmodel.transform.clickhouse.parser.tree.ClickHouseGenericDataType;
import com.aliyun.fastmodel.transform.clickhouse.parser.tree.ClickHouseTableType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.antlr.v4.runtime.tree.TerminalNode;

/**
 * ClickHouseParserBaseVisitor
 *
 * @author panguanjing
 * @date 2022/7/9
 */
public class ClickHouseAstBuilder extends ClickHouseParserBaseVisitor<Node> {

    private final ReverseContext context;

    public ClickHouseAstBuilder(ReverseContext context) {
        this.context = context;
    }

    @Override
    public Node visitQueryStmt(QueryStmtContext ctx) {
        if (ctx.query() != null) {
            return visit(ctx.query());
        }
        if (ctx.insertStmt() != null) {
            return visit(ctx.insertStmt());
        }
        return super.visitQueryStmt(ctx);
    }

    @Override
    public Node visitCreateTableStmt(CreateTableStmtContext ctx) {
        Boolean createOrReplace = ctx.REPLACE() != null;
        boolean ifNotExists = ctx.IF() != null && ctx.NOT() != null && ctx.EXISTS() != null;
        QualifiedName tableName = (QualifiedName)visit(ctx.tableIdentifier());
        ClickHouseTableType clickHouseTableType = ctx.TEMPORARY() != null ? ClickHouseTableType.TEMPORARY : ClickHouseTableType.NORMAL;
        List<ColumnDefinition> columnDefinitions = null;
        if (ctx.tableSchemaClause() != null) {
            TableSchemaClauseContext tableSchemaClauseContext = ctx.tableSchemaClause();
            Node node = visit(tableSchemaClauseContext);
            if (node instanceof QualifiedName) {
                return new CloneTable(CreateElement.builder().createOrReplace(createOrReplace).notExists(ifNotExists).qualifiedName(tableName)
                    .build(), clickHouseTableType, (QualifiedName)node);
            } else if (node instanceof ListNode) {
                ListNode listNode = (ListNode)node;
                columnDefinitions = toColumns(listNode);
            }
        }
        return CreateTable.builder()
            .createOrReplace(createOrReplace)
            .ifNotExist(ifNotExists)
            .tableName(tableName)
            .columns(columnDefinitions)
            .build();

    }

    private List<ColumnDefinition> toColumns(ListNode listNode) {
        List<? extends Node> children = listNode.getChildren();
        if (children == null || children.isEmpty()) {
            return ImmutableList.of();
        }
        return children.stream().filter(c -> c instanceof ColumnDefinition).map(c -> (ColumnDefinition)c).collect(Collectors.toList());
    }

    @Override
    public Node visitSchemaDescriptionClause(SchemaDescriptionClauseContext ctx) {
        List<TableElement> tableElementList = ParserHelper.visit(this, ctx.tableElementExpr(), TableElement.class);
        return new ListNode(tableElementList);
    }

    @Override
    public Node visitSchemaAsTableClause(SchemaAsTableClauseContext ctx) {
        return visit(ctx.tableIdentifier());
    }

    @Override
    public Node visitSchemaAsFunctionClause(SchemaAsFunctionClauseContext ctx) {
        return visit(ctx.tableFunctionExpr());
    }

    @Override
    public Node visitTableFunctionExpr(TableFunctionExprContext ctx) {
        Identifier identifier = (Identifier)visit(ctx.identifier());
        TableArgListContext tableArgListContext = ctx.tableArgList();
        List<BaseExpression> args = ImmutableList.of();
        if (tableArgListContext != null) {
            args = ParserHelper.visit(this, tableArgListContext.tableArgExpr(), BaseExpression.class);
        }
        return new FunctionCall(QualifiedName.of(Lists.newArrayList(identifier)), false, args);
    }

    @Override
    public Node visitTableElementExprColumn(TableElementExprColumnContext ctx) {
        return visit(ctx.tableColumnDfnt());
    }

    @Override
    public Node visitTableElementExprConstraint(TableElementExprConstraintContext ctx) {
        return super.visitTableElementExprConstraint(ctx);
    }

    @Override
    public Node visitTableElementExprIndex(TableElementExprIndexContext ctx) {
        return super.visitTableElementExprIndex(ctx);
    }

    @Override
    public Node visitTableColumnDfnt(TableColumnDfntContext ctx) {
        //only support one identifier
        Identifier identifier = (Identifier)visit(ctx.nestedIdentifier());
        BaseDataType baseDataType = ParserHelper.visitIfPresent(this, ctx.columnTypeExpr(), BaseDataType.class).orElse(null);
        Comment comment = null;
        if (ctx.COMMENT() != null) {
            StringLiteral stringLiteral = toStringLiteral(ctx.STRING_LITERAL());
            comment = new Comment(stringLiteral.getValue());
        }
        //default value
        BaseLiteral defaultValue = null;
        if (ctx.tableColumnPropertyExpr() != null) {
            boolean defaultNotNull = ctx.tableColumnPropertyExpr().DEFAULT() != null;
            if (defaultNotNull) {
                BaseExpression baseExpression = (BaseExpression)visit(ctx.tableColumnPropertyExpr().columnExpr());
                if (baseExpression instanceof BaseLiteral) {
                    defaultValue = (BaseLiteral)baseExpression;
                }
            }
        }
        return ColumnDefinition.builder().colName(identifier).dataType(baseDataType).comment(comment).defaultValue(defaultValue).build();
    }

    @Override
    public Node visitColumnTypeExprSimple(ColumnTypeExprSimpleContext ctx) {
        Identifier identifier = (Identifier)visit(ctx.identifier());
        return new ClickHouseGenericDataType(identifier);
    }

    @Override
    public Node visitColumnTypeExprNested(ColumnTypeExprNestedContext ctx) {
        return super.visitColumnTypeExprNested(ctx);
    }

    @Override
    public Node visitColumnTypeExprEnum(ColumnTypeExprEnumContext ctx) {
        Identifier identifier = (Identifier)visit(ctx.identifier());
        List<DataTypeParameter> parameters = ctx.enumValue().stream().map(c -> (DataTypeParameter)visit(c)).collect(Collectors.toList());
        return new ClickHouseGenericDataType(identifier, parameters);
    }

    @Override
    public Node visitEnumValue(EnumValueContext ctx) {
        StringLiteral stringLiteral = toStringLiteral(ctx.STRING_LITERAL());
        BaseLiteral baseLiteral = (BaseLiteral)visit(ctx.numberLiteral());
        return new ClickHouseEnumDataTypeParameter(stringLiteral, baseLiteral);
    }

    @Override
    public Node visitColumnTypeExprComplex(ColumnTypeExprComplexContext ctx) {
        Identifier identifier = (Identifier)visit(ctx.identifier());
        List<DataTypeParameter> list = ctx.columnTypeExpr().stream().map(t -> {
            BaseDataType baseDataType = (BaseDataType)visit(t);
            return (DataTypeParameter)new TypeParameter(baseDataType);
        }).collect(Collectors.toList());
        return new ClickHouseGenericDataType(identifier, list);
    }

    @Override
    public Node visitColumnTypeExprParam(ColumnTypeExprParamContext ctx) {
        Identifier identifier = (Identifier)visit(ctx.identifier());
        ColumnExprListContext columnExprListContext = ctx.columnExprList();
        List<DataTypeParameter> list = ImmutableList.of();
        if (columnExprListContext != null) {
            list = ParserHelper.visit(this, columnExprListContext.columnsExpr(), DataTypeParameter.class);
        }
        return new ClickHouseGenericDataType(identifier, list);
    }

    @Override
    public Node visitColumnsExprAsterisk(ColumnsExprAsteriskContext ctx) {
        TableIdentifierContext tableIdentifierContext = ctx.tableIdentifier();
        if (tableIdentifierContext != null) {
            QualifiedName tableName = (QualifiedName)visit(tableIdentifierContext);
            return new AllColumns(new TableOrColumn(tableName));
        }
        return new AllColumns(null);
    }

    @Override
    public Node visitColumnsExprColumn(ColumnsExprColumnContext ctx) {
        return visit(ctx.columnExpr());
    }

    @Override
    public Node visitColumnExprLiteral(ColumnExprLiteralContext ctx) {
        return visit(ctx.literal());
    }

    @Override
    public Node visitLiteral(LiteralContext ctx) {
        boolean isNumberLiteral = ctx.numberLiteral() != null;
        if (isNumberLiteral) {
            if (ctx.numberLiteral().floatingLiteral() != null) {
                return new DoubleLiteral(ctx.getText());
            }
            if (ctx.numberLiteral().DECIMAL_LITERAL() != null) {
                return new LongLiteral(ctx.getText());
            }
            if (ctx.numberLiteral().HEXADECIMAL_LITERAL() != null) {
                return new HexLiteral(ctx.numberLiteral().getText());
            }
        }
        if (ctx.STRING_LITERAL() != null) {
            return new StringLiteral(StripUtils.strip(ctx.STRING_LITERAL().getText()));
        }
        if (ctx.NULL_SQL() != null) {
            return new NullLiteral();
        }
        return super.visitLiteral(ctx);
    }

    private StringLiteral toStringLiteral(TerminalNode stringLiteral) {
        return new StringLiteral(StripUtils.strip(stringLiteral.getText()));
    }

    @Override
    public Node visitNestedIdentifier(NestedIdentifierContext ctx) {
        List<Identifier> identifier = ParserHelper.visit(this, ctx.identifier(), Identifier.class);
        QualifiedName qualifiedName = QualifiedName.of(identifier);
        return new Identifier(qualifiedName.getSuffix());
    }

    @Override
    public Node visitTableIdentifier(TableIdentifierContext ctx) {
        List<Identifier> list = Lists.newArrayList();
        boolean existDatabase = ctx.databaseIdentifier() != null;
        if (existDatabase) {
            Identifier database = (Identifier)visit(ctx.databaseIdentifier());
            list.add(database);
        }
        Identifier name = (Identifier)visit(ctx.identifier());
        list.add(name);
        return QualifiedName.of(list);
    }

    @Override
    public Node visitDatabaseIdentifier(DatabaseIdentifierContext ctx) {
        return visit(ctx.identifier());
    }

    @Override
    public Node visitIdentifier(IdentifierContext ctx) {
        return ParserHelper.getIdentifier(ctx);
    }
}
