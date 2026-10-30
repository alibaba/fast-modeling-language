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
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeParameter;
import com.aliyun.fastmodel.core.tree.datatype.NumericParameter;
import com.aliyun.fastmodel.core.tree.datatype.TypeParameter;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.atom.FunctionCall;
import com.aliyun.fastmodel.core.tree.expr.atom.TableOrColumn;
import com.aliyun.fastmodel.core.tree.expr.literal.DecimalLiteral;
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
import com.aliyun.fastmodel.core.tree.statement.table.constraint.BaseConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.PrimaryConstraint;
import com.aliyun.fastmodel.core.tree.util.IdentifierUtil;
import com.aliyun.fastmodel.transform.api.context.ReverseContext;
import com.aliyun.fastmodel.transform.clickhouse.format.ClickHousePropertyKey;
import com.aliyun.fastmodel.transform.clickhouse.parser.ClickHouseParser.AlterTableClauseAttachContext;
import com.aliyun.fastmodel.transform.clickhouse.parser.ClickHouseParser.AlterTableClauseContext;
import com.aliyun.fastmodel.transform.clickhouse.parser.ClickHouseParser.AlterTableClauseDetachContext;
import com.aliyun.fastmodel.transform.clickhouse.parser.ClickHouseParser.AlterTableClauseDropPartitionContext;
import com.aliyun.fastmodel.transform.clickhouse.parser.ClickHouseParser.AlterTableClauseFreezePartitionContext;
import com.aliyun.fastmodel.transform.clickhouse.parser.ClickHouseParser.AlterTableClauseMovePartitionContext;
import com.aliyun.fastmodel.transform.clickhouse.parser.ClickHouseParser.AlterTableClauseReplaceContext;
import com.aliyun.fastmodel.transform.clickhouse.parser.ClickHouseParser.AlterTableStmtContext;
import com.aliyun.fastmodel.transform.clickhouse.parser.ClickHouseParser.PartitionClauseContext;
import com.aliyun.fastmodel.transform.clickhouse.parser.ClickHouseParser.ColumnExprListContext;
import com.aliyun.fastmodel.transform.clickhouse.parser.ClickHouseParser.ColumnExprLiteralContext;
import com.aliyun.fastmodel.transform.clickhouse.parser.ClickHouseParser.ColumnTypeExprComplexContext;
import com.aliyun.fastmodel.transform.clickhouse.parser.ClickHouseParser.ColumnTypeExprEnumContext;
import com.aliyun.fastmodel.transform.clickhouse.parser.ClickHouseParser.ColumnTypeExprNestedContext;
import com.aliyun.fastmodel.transform.clickhouse.parser.ClickHouseParser.ColumnTypeExprParamContext;
import com.aliyun.fastmodel.transform.clickhouse.parser.ClickHouseParser.ColumnTypeExprSimpleContext;
import com.aliyun.fastmodel.transform.clickhouse.parser.ClickHouseParser.ColumnsExprAsteriskContext;
import com.aliyun.fastmodel.transform.clickhouse.parser.ClickHouseParser.ColumnsExprColumnContext;
import com.aliyun.fastmodel.transform.clickhouse.parser.ClickHouseParser.ColumnsExprContext;
import com.aliyun.fastmodel.transform.clickhouse.parser.ClickHouseParser.ClusterClauseContext;
import com.aliyun.fastmodel.transform.clickhouse.parser.ClickHouseParser.CreateTableStmtContext;
import com.aliyun.fastmodel.transform.clickhouse.parser.ClickHouseParser.DatabaseIdentifierContext;
import com.aliyun.fastmodel.transform.clickhouse.parser.ClickHouseParser.EngineClauseContext;
import com.aliyun.fastmodel.transform.clickhouse.parser.ClickHouseParser.EngineExprContext;
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
import com.aliyun.fastmodel.transform.clickhouse.parser.ClickHouseParser.TableCommentClauseContext;
import com.aliyun.fastmodel.transform.clickhouse.parser.ClickHouseParser.TableElementExprColumnContext;
import com.aliyun.fastmodel.transform.clickhouse.parser.ClickHouseParser.TableElementExprConstraintContext;
import com.aliyun.fastmodel.transform.clickhouse.parser.ClickHouseParser.TableElementExprIndexContext;
import com.aliyun.fastmodel.transform.clickhouse.parser.ClickHouseParser.TableElementExprProjectionContext;
import com.aliyun.fastmodel.transform.clickhouse.parser.ClickHouseParser.TableFunctionExprContext;
import com.aliyun.fastmodel.transform.clickhouse.parser.ClickHouseParser.TableIdentifierContext;
import com.aliyun.fastmodel.transform.clickhouse.parser.ClickHouseParser.TableSchemaClauseContext;
import com.aliyun.fastmodel.transform.clickhouse.parser.ClickHouseParserBaseVisitor;
import com.aliyun.fastmodel.transform.clickhouse.parser.tree.ClickHouseAlterTable;
import com.aliyun.fastmodel.transform.clickhouse.parser.tree.ClickHouseAlterTableClause;
import com.aliyun.fastmodel.transform.clickhouse.parser.tree.ClickHouseAlterTableClause.ClauseType;
import com.aliyun.fastmodel.transform.clickhouse.parser.tree.ClickHouseDataTypeName;
import com.aliyun.fastmodel.transform.clickhouse.parser.tree.ClickHouseEnumDataTypeParameter;
import com.aliyun.fastmodel.transform.clickhouse.parser.tree.ClickHouseGenericDataType;
import com.aliyun.fastmodel.transform.clickhouse.parser.tree.ClickHouseTableType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * ClickHouseParserBaseVisitor
 *
 * @author panguanjing
 * @date 2022/7/9
 */
public class ClickHouseAstBuilder extends ClickHouseParserBaseVisitor<Node> {

    private static final String BACK_QUOTE = "`";

    private static final String DOUBLE_QUOTE = "\"";

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
    public Node visitAlterTableStmt(AlterTableStmtContext ctx) {
        QualifiedName tableName = (QualifiedName)visit(ctx.tableIdentifier());
        String cluster = null;
        if (ctx.clusterClause() != null) {
            cluster = stripKeyword(ParserHelper.getOrigin(ctx.clusterClause()), "ON CLUSTER");
        }
        List<ClickHouseAlterTableClause> clauses = Lists.newArrayList();
        for (AlterTableClauseContext alterTableClauseContext : ctx.alterTableClause()) {
            ClickHouseAlterTableClause clause = toAlterTableClause(alterTableClauseContext);
            if (clause != null) {
                clauses.add(clause);
            }
        }
        return ClickHouseAlterTable.builder()
            .tableName(tableName)
            .cluster(cluster)
            .clauses(clauses)
            .build();
    }

    /**
     * 只处理分区相关的子句，其余 ALTER 子句返回空由调用方忽略
     */
    private ClickHouseAlterTableClause toAlterTableClause(AlterTableClauseContext ctx) {
        if (ctx instanceof AlterTableClauseDropPartitionContext) {
            return ClickHouseAlterTableClause.builder()
                .type(ClauseType.DROP_PARTITION)
                .partition(toPartitionValue(ctx))
                .build();
        }
        if (ctx instanceof AlterTableClauseDetachContext) {
            return ClickHouseAlterTableClause.builder()
                .type(ClauseType.DETACH_PARTITION)
                .partition(toPartitionValue(ctx))
                .build();
        }
        if (ctx instanceof AlterTableClauseAttachContext) {
            AlterTableClauseAttachContext attachContext = (AlterTableClauseAttachContext)ctx;
            return ClickHouseAlterTableClause.builder()
                .type(ClauseType.ATTACH_PARTITION)
                .partition(toPartitionValue(ctx))
                .fromTable(attachContext.tableIdentifier() == null ? null : ParserHelper.getOrigin(attachContext.tableIdentifier()))
                .build();
        }
        if (ctx instanceof AlterTableClauseFreezePartitionContext) {
            return ClickHouseAlterTableClause.builder()
                .type(ClauseType.FREEZE)
                .partition(toPartitionValue(ctx))
                .build();
        }
        if (ctx instanceof AlterTableClauseReplaceContext) {
            AlterTableClauseReplaceContext replaceContext = (AlterTableClauseReplaceContext)ctx;
            return ClickHouseAlterTableClause.builder()
                .type(ClauseType.REPLACE_PARTITION)
                .partition(toPartitionValue(ctx))
                .fromTable(replaceContext.tableIdentifier() == null ? null : ParserHelper.getOrigin(replaceContext.tableIdentifier()))
                .build();
        }
        if (ctx instanceof AlterTableClauseMovePartitionContext) {
            AlterTableClauseMovePartitionContext moveContext = (AlterTableClauseMovePartitionContext)ctx;
            if (moveContext.DISK() != null) {
                return ClickHouseAlterTableClause.builder()
                    .type(ClauseType.MOVE_PARTITION_TO_DISK)
                    .partition(toPartitionValue(ctx))
                    .target(ParserHelper.getOrigin(moveContext.STRING_LITERAL()))
                    .build();
            }
            if (moveContext.VOLUME() != null) {
                return ClickHouseAlterTableClause.builder()
                    .type(ClauseType.MOVE_PARTITION_TO_VOLUME)
                    .partition(toPartitionValue(ctx))
                    .target(ParserHelper.getOrigin(moveContext.STRING_LITERAL()))
                    .build();
            }
            if (moveContext.TABLE() != null) {
                return ClickHouseAlterTableClause.builder()
                    .type(ClauseType.MOVE_PARTITION_TO_TABLE)
                    .partition(toPartitionValue(ctx))
                    .target(ParserHelper.getOrigin(moveContext.tableIdentifier()))
                    .build();
            }
        }
        return null;
    }

    private String toPartitionValue(AlterTableClauseContext ctx) {
        PartitionClauseContext partitionClauseContext = ctx.getRuleContext(PartitionClauseContext.class, 0);
        if (partitionClauseContext == null) {
            return null;
        }
        return stripKeyword(ParserHelper.getOrigin(partitionClauseContext), "PARTITION");
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
        List<Property> properties = toProperties(ctx);
        List<BaseConstraint> constraints = toPrimaryConstraints(properties, columnDefinitions);
        return CreateTable.builder()
            .createOrReplace(createOrReplace)
            .ifNotExist(ifNotExists)
            .tableName(tableName)
            .columns(columnDefinitions)
            .constraints(constraints)
            .properties(properties)
            .comment(toTableComment(ctx.tableCommentClause()))
            .build();

    }

    /**
     * ClickHouse的引擎子句(ENGINE/PARTITION BY/PRIMARY KEY/ORDER BY/SAMPLE BY/TTL/SETTINGS)与
     * ON CLUSTER 统一落到表属性,原样保留表达式文本,便于反向再生成建表语句
     *
     * @param createTableStmtContext 建表语句
     * @return 表属性
     */
    private List<Property> toProperties(CreateTableStmtContext createTableStmtContext) {
        List<Property> properties = Lists.newArrayList();
        ClusterClauseContext clusterClauseContext = createTableStmtContext.clusterClause();
        if (clusterClauseContext != null) {
            addProperty(properties, ClickHousePropertyKey.TABLE_CLUSTER,
                stripKeyword(ParserHelper.getOrigin(clusterClauseContext), "ON CLUSTER"));
        }
        EngineClauseContext engineClauseContext = createTableStmtContext.engineClause();
        if (engineClauseContext == null) {
            return properties;
        }
        EngineExprContext engineExprContext = engineClauseContext.engineExpr();
        if (engineExprContext != null) {
            addProperty(properties, ClickHousePropertyKey.TABLE_ENGINE, toEngineValue(engineExprContext));
        }
        addProperty(properties, ClickHousePropertyKey.TABLE_PARTITION_BY,
            getClauseValue(engineClauseContext.partitionByClause(), "PARTITION BY"));
        addProperty(properties, ClickHousePropertyKey.TABLE_PRIMARY_KEY,
            getClauseValue(engineClauseContext.primaryKeyClause(), "PRIMARY KEY"));
        addProperty(properties, ClickHousePropertyKey.TABLE_ORDER_BY,
            getClauseValue(engineClauseContext.orderByClause(), "ORDER BY"));
        addProperty(properties, ClickHousePropertyKey.TABLE_SAMPLE_BY,
            getClauseValue(engineClauseContext.sampleByClause(), "SAMPLE BY"));
        addProperty(properties, ClickHousePropertyKey.TABLE_TTL,
            getClauseValue(engineClauseContext.ttlClause(), "TTL"));
        addProperty(properties, ClickHousePropertyKey.TABLE_SETTINGS,
            getClauseValue(engineClauseContext.settingsClause(), "SETTINGS"));
        return properties;
    }

    private String getClauseValue(List<? extends ParserRuleContext> clauses, String keyword) {
        if (CollectionUtils.isEmpty(clauses)) {
            return null;
        }
        return stripKeyword(ParserHelper.getOrigin(clauses.get(0)), keyword);
    }

    private String toEngineValue(EngineExprContext engineExprContext) {
        String origin = StringUtils.trimToEmpty(ParserHelper.getOrigin(engineExprContext));
        int index = origin.indexOf('=');
        if (index > 0) {
            return origin.substring(index + 1).trim();
        }
        return stripKeyword(origin, "ENGINE");
    }

    private String stripKeyword(String origin, String keyword) {
        String value = StringUtils.trimToEmpty(origin);
        if (StringUtils.startsWithIgnoreCase(value, keyword)) {
            value = value.substring(keyword.length()).trim();
        }
        return value;
    }

    private void addProperty(List<Property> properties, ClickHousePropertyKey propertyKey, String value) {
        if (StringUtils.isBlank(value)) {
            return;
        }
        properties.add(new Property(propertyKey.getValue(), value));
    }

    private Comment toTableComment(TableCommentClauseContext tableCommentClauseContext) {
        if (tableCommentClauseContext == null) {
            return null;
        }
        StringLiteral stringLiteral = toStringLiteral(tableCommentClauseContext.STRING_LITERAL());
        return new Comment(stringLiteral.getValue());
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
        throw new UnsupportedOperationException("ClickHouse table CONSTRAINT is not supported by the FastModel AST");
    }

    @Override
    public Node visitTableElementExprIndex(TableElementExprIndexContext ctx) {
        throw new UnsupportedOperationException("ClickHouse table INDEX is not supported by the FastModel AST");
    }

    @Override
    public Node visitTableElementExprProjection(TableElementExprProjectionContext ctx) {
        throw new UnsupportedOperationException("ClickHouse table PROJECTION is not supported by the FastModel AST");
    }

    @Override
    public Node visitTableColumnDfnt(TableColumnDfntContext ctx) {
        //only support one identifier
        Identifier identifier = (Identifier)visit(ctx.nestedIdentifier());
        BaseDataType baseDataType = ParserHelper.visitIfPresent(this, ctx.columnTypeExpr(), BaseDataType.class).orElse(null);
        boolean nullable = false;
        if (isNullable(baseDataType)) {
            nullable = true;
            baseDataType = unwrapNullable((ClickHouseGenericDataType)baseDataType);
        }
        Comment comment = null;
        if (ctx.COMMENT() != null) {
            StringLiteral stringLiteral = toStringLiteral(ctx.STRING_LITERAL());
            comment = new Comment(stringLiteral.getValue());
        }
        //default value
        BaseExpression defaultValue = null;
        if (ctx.tableColumnPropertyExpr() != null) {
            if (ctx.tableColumnPropertyExpr().DEFAULT() == null) {
                throw new UnsupportedOperationException("ClickHouse MATERIALIZED and ALIAS columns are not supported by the FastModel AST");
            }
            defaultValue = (BaseExpression)visit(ctx.tableColumnPropertyExpr().columnExpr());
            if (defaultValue == null) {
                throw new UnsupportedOperationException("ClickHouse DEFAULT expression is not supported by the FastModel AST");
            }
        }
        if (ctx.codecExpr() != null || ctx.TTL() != null) {
            throw new UnsupportedOperationException("ClickHouse column CODEC and TTL are not supported by the FastModel AST");
        }
        return ColumnDefinition.builder().colName(identifier).dataType(baseDataType).comment(comment).notNull(!nullable)
            .defaultValue(defaultValue).build();
    }

    private List<BaseConstraint> toPrimaryConstraints(List<Property> properties, List<ColumnDefinition> columns) {
        String primaryKey = properties.stream()
            .filter(property -> StringUtils.equalsIgnoreCase(property.getName(), ClickHousePropertyKey.TABLE_PRIMARY_KEY.getValue()))
            .map(Property::getValue)
            .findFirst()
            .orElse(null);
        List<Identifier> primaryColumns = toPrimaryColumns(primaryKey);
        if (CollectionUtils.isEmpty(primaryColumns)) {
            return Lists.newArrayList();
        }
        if (CollectionUtils.isNotEmpty(columns)) {
            for (ColumnDefinition column : columns) {
                if (primaryColumns.stream().anyMatch(primary -> StringUtils.equals(primary.getValue(), column.getColName().getValue()))) {
                    column.setPrimary(true);
                }
            }
        }
        return Lists.newArrayList(new PrimaryConstraint(IdentifierUtil.sysIdentifier(), primaryColumns));
    }

    private List<Identifier> toPrimaryColumns(String value) {
        if (StringUtils.isBlank(value)) {
            return Lists.newArrayList();
        }
        String expression = value.trim();
        if (expression.startsWith("(") && expression.endsWith(")")) {
            expression = expression.substring(1, expression.length() - 1).trim();
        }
        String[] names = expression.split(",");
        List<Identifier> columns = Lists.newArrayList();
        for (String name : names) {
            String candidate = name.trim();
            if (!candidate.matches("(?:[A-Za-z_][A-Za-z0-9_]*|`[^`]+`|\"[^\"]+\")")) {
                return Lists.newArrayList();
            }
            columns.add(new Identifier(StripUtils.strip(candidate)));
        }
        return columns;
    }

    private boolean isNullable(BaseDataType baseDataType) {
        if (!(baseDataType instanceof ClickHouseGenericDataType)) {
            return false;
        }
        ClickHouseGenericDataType genericDataType = (ClickHouseGenericDataType)baseDataType;
        return StringUtils.equalsIgnoreCase(genericDataType.getName(), ClickHouseDataTypeName.Nullable.getValue());
    }

    /**
     * Nullable(T) 的可空语义由列的 notNull 承载,类型还原为 T,生成 DDL 时再按需包装
     */
    private BaseDataType unwrapNullable(ClickHouseGenericDataType genericDataType) {
        List<DataTypeParameter> arguments = genericDataType.getArguments();
        if (CollectionUtils.isEmpty(arguments)) {
            return genericDataType;
        }
        DataTypeParameter argument = arguments.get(0);
        if (!(argument instanceof TypeParameter)) {
            return genericDataType;
        }
        BaseDataType dataType = ((TypeParameter)argument).getType();
        return dataType == null ? genericDataType : dataType;
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
        //枚举值可能是负数，按原始文本保留，避免丢失符号与精度
        DecimalLiteral numberValue = new DecimalLiteral(ParserHelper.getOrigin(ctx.numberLiteral()));
        return new ClickHouseEnumDataTypeParameter(stringLiteral, numberValue);
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
        List<DataTypeParameter> list = Lists.newArrayList();
        if (columnExprListContext != null) {
            for (ColumnsExprContext columnsExprContext : columnExprListContext.columnsExpr()) {
                DataTypeParameter dataTypeParameter = toDataTypeParameter(columnsExprContext);
                if (dataTypeParameter != null) {
                    list.add(dataTypeParameter);
                }
            }
        }
        return new ClickHouseGenericDataType(identifier, list);
    }

    /**
     * ClickHouse的类型参数既可能是数值(Decimal(10,2))、字符串(DateTime('Asia/Shanghai'))，也可能是类型(Array(String))
     */
    private DataTypeParameter toDataTypeParameter(ColumnsExprContext columnsExprContext) {
        Node node = visit(columnsExprContext);
        if (node instanceof DataTypeParameter) {
            return (DataTypeParameter)node;
        }
        if (node instanceof BaseDataType) {
            return new TypeParameter((BaseDataType)node);
        }
        if (node instanceof LongLiteral) {
            return new NumericParameter(String.valueOf(((LongLiteral)node).getValue()));
        }
        if (node instanceof DoubleLiteral) {
            return new NumericParameter(String.valueOf(((DoubleLiteral)node).getValue()));
        }
        String origin = ParserHelper.getOrigin(columnsExprContext);
        if (StringUtils.isBlank(origin)) {
            return null;
        }
        return new NumericParameter(origin);
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
        if (identifier.size() == 1) {
            //单列场景保留原始标识符对象，避免丢失反引号等引用形态
            return identifier.get(0);
        }
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
        String text = ctx.getText();
        //ClickHouse 的标识符大小写敏感，未加引号时也保留原始大小写
        if (StringUtils.startsWithAny(text, BACK_QUOTE, DOUBLE_QUOTE)) {
            return ParserHelper.getIdentifier(ctx);
        }
        return new Identifier(ParserHelper.getLocation(ctx), ParserHelper.getOrigin(ctx), text);
    }
}
