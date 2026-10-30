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

package com.aliyun.fastmodel.transform.clickhouse.format;

import java.util.List;

import com.aliyun.fastmodel.core.formatter.FastModelVisitor;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.PartitionedBy;
import com.aliyun.fastmodel.core.tree.util.PropertyUtil;
import com.aliyun.fastmodel.transform.clickhouse.context.ClickHouseContext;
import com.aliyun.fastmodel.transform.clickhouse.parser.tree.ClickHouseAlterTable;
import com.aliyun.fastmodel.transform.clickhouse.parser.tree.ClickHouseAlterTableClause;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;

import static com.aliyun.fastmodel.transform.clickhouse.format.ClickHousePropertyKey.TABLE_CLUSTER;
import static com.aliyun.fastmodel.transform.clickhouse.format.ClickHousePropertyKey.TABLE_ENGINE;
import static com.aliyun.fastmodel.transform.clickhouse.format.ClickHousePropertyKey.TABLE_ORDER_BY;
import static com.aliyun.fastmodel.transform.clickhouse.format.ClickHousePropertyKey.TABLE_PARTITION_BY;
import static com.aliyun.fastmodel.transform.clickhouse.format.ClickHousePropertyKey.TABLE_PRIMARY_KEY;
import static com.aliyun.fastmodel.transform.clickhouse.format.ClickHousePropertyKey.TABLE_SAMPLE_BY;
import static com.aliyun.fastmodel.transform.clickhouse.format.ClickHousePropertyKey.TABLE_SETTINGS;
import static com.aliyun.fastmodel.transform.clickhouse.format.ClickHousePropertyKey.TABLE_TTL;

/**
 * clickHouse建表语句的输出
 * <p>
 * ClickHouse 建表语法为 CREATE TABLE (columns) ENGINE = ... [PARTITION BY ...] [PRIMARY KEY ...]
 * [ORDER BY ...] [SAMPLE BY ...] [TTL ...] [SETTINGS ...] [COMMENT ...]，
 * 表级子句统一来自表属性(见 {@link ClickHousePropertyKey})。
 *
 * @author panguanjing
 */
public class ClickHouseOutVisitor extends FastModelVisitor {

    private static final String NULLABLE = "Nullable(";

    private static final String LOW_CARDINALITY = "LowCardinality(";

    private final ClickHouseContext clickHouseContext;

    public ClickHouseOutVisitor(ClickHouseContext clickHouseContext) {
        this.clickHouseContext = clickHouseContext;
    }

    @Override
    public Boolean visitCreateTable(CreateTable node, Integer indent) {
        boolean columnEmpty = node.isColumnEmpty();
        builder.append(BooleanUtils.isTrue(node.getCreateOrReplace()) ? "CREATE OR REPLACE TABLE " : "CREATE TABLE ");
        if (node.isNotExists()) {
            builder.append("IF NOT EXISTS ");
        }
        String tableName = getCode(node.getQualifiedName());
        validateDdlFragment(tableName, "table name");
        builder.append(tableName);
        String cluster = getPropertyValue(node.getProperties(), TABLE_CLUSTER);
        if (StringUtils.isNotBlank(cluster)) {
            validateDdlFragment(cluster, "ON CLUSTER");
            builder.append(" ON CLUSTER ").append(cluster);
        }
        if (!columnEmpty) {
            builder.append("\n(\n");
            builder.append(formatColumnList(node.getColumnDefines(), indentString(indent + 1)));
            builder.append("\n)");
        }
        List<Property> properties = node.getProperties();
        appendClause("ENGINE = ", getPropertyValue(properties, TABLE_ENGINE));
        appendClause("PARTITION BY ", getPartitionBy(node));
        appendClause("PRIMARY KEY ", getPropertyValue(properties, TABLE_PRIMARY_KEY));
        appendClause("ORDER BY ", getPropertyValue(properties, TABLE_ORDER_BY));
        appendClause("SAMPLE BY ", getPropertyValue(properties, TABLE_SAMPLE_BY));
        appendClause("TTL ", getPropertyValue(properties, TABLE_TTL));
        appendClause("SETTINGS ", getPropertyValue(properties, TABLE_SETTINGS));
        if (node.getComment() != null && StringUtils.isNotBlank(node.getCommentValue())) {
            builder.append("\n");
            builder.append(formatComment(node.getComment(), true));
        }
        builder.append(SUFFIX);
        return !columnEmpty;
    }

    /**
     * 输出 ALTER TABLE 的分区运维语句
     *
     * @param statement ALTER 语句
     * @return SQL
     */
    public String formatAlterTable(ClickHouseAlterTable statement) {
        StringBuilder sql = new StringBuilder("ALTER TABLE ");
        String tableName = getCode(statement.getTableName());
        validateDdlFragment(tableName, "table name");
        sql.append(tableName);
        if (StringUtils.isNotBlank(statement.getCluster())) {
            validateDdlFragment(statement.getCluster(), "ON CLUSTER");
            sql.append(" ON CLUSTER ").append(statement.getCluster());
        }
        List<String> clauses = Lists.newArrayList();
        for (ClickHouseAlterTableClause clause : statement.getClauses()) {
            clauses.add(formatAlterClause(clause));
        }
        sql.append(" ").append(String.join(", ", clauses)).append(SUFFIX);
        return sql.toString();
    }

    private String formatAlterClause(ClickHouseAlterTableClause clause) {
        String partition = clause.getPartition();
        if (StringUtils.isNotBlank(partition)) {
            validateDdlFragment(partition, "partition");
        }
        switch (clause.getType()) {
            case DROP_PARTITION:
                return "DROP PARTITION " + partition;
            case DETACH_PARTITION:
                return "DETACH PARTITION " + partition;
            case ATTACH_PARTITION:
                return appendFrom("ATTACH PARTITION " + partition, clause.getFromTable());
            case FREEZE:
                return StringUtils.isBlank(partition) ? "FREEZE" : "FREEZE PARTITION " + partition;
            case MOVE_PARTITION_TO_DISK:
                return "MOVE PARTITION " + partition + " TO DISK " + validatedTarget(clause.getTarget());
            case MOVE_PARTITION_TO_VOLUME:
                return "MOVE PARTITION " + partition + " TO VOLUME " + validatedTarget(clause.getTarget());
            case MOVE_PARTITION_TO_TABLE:
                return "MOVE PARTITION " + partition + " TO TABLE " + validatedTarget(clause.getTarget());
            case REPLACE_PARTITION:
                return appendFrom("REPLACE PARTITION " + partition, clause.getFromTable());
            default:
                throw new UnsupportedOperationException("unsupported alter clause type:" + clause.getType());
        }
    }

    private String appendFrom(String prefix, String fromTable) {
        if (StringUtils.isBlank(fromTable)) {
            return prefix;
        }
        validateDdlFragment(fromTable, "FROM table");
        return prefix + " FROM " + fromTable;
    }

    private String validatedTarget(String target) {
        validateDdlFragment(target, "move target");
        return target;
    }

    /**
     * 分区键优先取显式的 partition_by 表达式；未指定时按分区列生成 (col1,col2)
     */
    private String getPartitionBy(CreateTable node) {
        String partitionBy = getPropertyValue(node.getProperties(), TABLE_PARTITION_BY);
        if (StringUtils.isNotBlank(partitionBy)) {
            return partitionBy;
        }
        PartitionedBy partitionedBy = node.getPartitionedBy();
        if (partitionedBy == null || CollectionUtils.isEmpty(partitionedBy.getColumnDefinitions())) {
            return null;
        }
        StringBuilder stringBuilder = new StringBuilder("(");
        List<ColumnDefinition> columnDefinitions = partitionedBy.getColumnDefinitions();
        for (int i = 0; i < columnDefinitions.size(); i++) {
            if (i > 0) {
                stringBuilder.append(",");
            }
            stringBuilder.append(formatExpression(columnDefinitions.get(i).getColName()));
        }
        stringBuilder.append(")");
        return stringBuilder.toString();
    }

    private void appendClause(String prefix, String value) {
        if (StringUtils.isBlank(value)) {
            return;
        }
        validateDdlFragment(value, prefix);
        builder.append("\n").append(prefix).append(value);
    }

    private String getPropertyValue(List<Property> properties, ClickHousePropertyKey propertyKey) {
        if (CollectionUtils.isEmpty(properties)) {
            return null;
        }
        return PropertyUtil.getPropertyValue(properties, propertyKey.getValue());
    }

    @Override
    protected String formatColumnDefinition(ColumnDefinition column, Integer max) {
        String col = formatColName(column.getColName(), max);
        validateDdlFragment(col, "column name");
        StringBuilder sb = new StringBuilder().append(col);
        BaseDataType dataType = column.getDataType();
        if (dataType != null) {
            sb.append(" ").append(formatClickHouseDataType(column, dataType));
        }
        if (column.getDefaultValue() != null) {
            sb.append(" DEFAULT ").append(formatExpression(column.getDefaultValue()));
        }
        sb.append(formatComment(column.getComment(), isEndNewLine(sb.toString())));
        return sb.toString();
    }

    /**
     * ClickHouse 的可空类型通过 Nullable(T) 表达,非空列直接使用原始类型
     */
    private String formatClickHouseDataType(ColumnDefinition column, BaseDataType dataType) {
        String type = formatDataType(dataType);
        if (BooleanUtils.isFalse(column.getNotNull()) && !isNullableType(type) && !isPrimaryKey(column)) {
            if (isNullableContainerType(type)) {
                throw new IllegalArgumentException("ClickHouse container type cannot be wrapped in Nullable: " + type);
            }
            return NULLABLE + type + ")";
        }
        return type;
    }

    private boolean isNullableType(String type) {
        return StringUtils.startsWithIgnoreCase(type, NULLABLE) || StringUtils.startsWithIgnoreCase(type, LOW_CARDINALITY + NULLABLE);
    }

    private boolean isNullableContainerType(String type) {
        return StringUtils.startsWithIgnoreCase(type, "Array(")
            || StringUtils.startsWithIgnoreCase(type, "Map(")
            || StringUtils.startsWithIgnoreCase(type, "Tuple(")
            || StringUtils.startsWithIgnoreCase(type, "Nested(");
    }

    private boolean isPrimaryKey(ColumnDefinition column) {
        return BooleanUtils.isTrue(column.getPrimary());
    }

    private void validateDdlFragment(String value, String field) {
        if (StringUtils.isBlank(value)) {
            return;
        }
        if (value.indexOf(';') >= 0 || value.indexOf('\n') >= 0 || value.indexOf('\r') >= 0
            || value.indexOf('\u0000') >= 0 || value.contains("--") || value.contains("/*") || value.contains("*/")) {
            throw new IllegalArgumentException("Unsafe ClickHouse " + field + " value");
        }
    }

    @Override
    protected String formatStringLiteral(String s) {
        return new ClickHouseExpressionVisitor(clickHouseContext).formatStringLiteral(s);
    }

    @Override
    protected String formatExpression(BaseExpression baseExpression) {
        return new ClickHouseExpressionVisitor(clickHouseContext).process(baseExpression);
    }
}
