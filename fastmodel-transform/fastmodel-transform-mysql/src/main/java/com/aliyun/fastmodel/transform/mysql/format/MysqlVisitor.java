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

package com.aliyun.fastmodel.transform.mysql.format;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.core.formatter.ExpressionFormatter;
import com.aliyun.fastmodel.core.formatter.FastModelVisitor;
import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums;
import com.aliyun.fastmodel.core.tree.datatype.GenericDataType;
import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName;
import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName.Dimension;
import com.aliyun.fastmodel.core.tree.datatype.NumericParameter;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.BaseOperatorStatement;
import com.aliyun.fastmodel.core.tree.statement.CompositeStatement;
import com.aliyun.fastmodel.core.tree.statement.constants.ConstraintType;
import com.aliyun.fastmodel.core.tree.statement.element.MultiComment;
import com.aliyun.fastmodel.core.tree.statement.script.RefObject;
import com.aliyun.fastmodel.core.tree.statement.script.RefRelation;
import com.aliyun.fastmodel.core.tree.statement.table.AddCols;
import com.aliyun.fastmodel.core.tree.statement.table.AddConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.AddPartitionCol;
import com.aliyun.fastmodel.core.tree.statement.table.ChangeCol;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.DropCol;
import com.aliyun.fastmodel.core.tree.statement.table.DropConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.DropPartitionCol;
import com.aliyun.fastmodel.core.tree.statement.table.RenameCol;
import com.aliyun.fastmodel.core.tree.statement.table.SetColComment;
import com.aliyun.fastmodel.core.tree.statement.table.SetTableComment;
import com.aliyun.fastmodel.core.tree.statement.table.SetTableProperties;
import com.aliyun.fastmodel.core.tree.statement.table.UnSetTableProperties;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.BaseConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.DimConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.PrimaryConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.UniqueConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.index.TableIndex;
import com.aliyun.fastmodel.transform.api.datatype.DataTypeConverter;
import com.aliyun.fastmodel.transform.api.format.DefaultExpressionVisitor;
import com.aliyun.fastmodel.transform.api.util.StringJoinUtil;
import com.aliyun.fastmodel.transform.mysql.context.MysqlTransformContext;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;

import static java.util.stream.Collectors.joining;

/**
 * 基于FML的visitor处理内容
 *
 * @author panguanjing
 * @date 2021/6/24
 */
public class MysqlVisitor extends FastModelVisitor {

    private final MysqlTransformContext mysqlTransformContext;

    private final DataTypeConverter dataTypeTransformer;

    // Map to hold grouped alter table operations by table name
    private final Map<QualifiedName, List<BaseStatement>> groupedAlterOperations = Maps.newConcurrentMap();

    public MysqlVisitor(MysqlTransformContext mysqlTransformContext) {
        if (mysqlTransformContext == null) {
            this.mysqlTransformContext = MysqlTransformContext.builder().build();
        } else {
            this.mysqlTransformContext = mysqlTransformContext;
        }
        dataTypeTransformer = this.mysqlTransformContext.getDataTypeTransformer();
    }

    @Override
    public Boolean visitCreateTable(CreateTable node, Integer indent) {
        boolean columnEmpty = node.isColumnEmpty();
        // maxcompute不支持没有列的表
        boolean executable = true;
        if (columnEmpty) {
            executable = false;
        }
        builder.append("CREATE TABLE ");
        if (node.isNotExists()) {
            builder.append("IF NOT EXISTS ");
        }
        String tableName = node.getIdentifier();
        builder.append(tableName);
        if (!columnEmpty) {
            builder.append("\n(\n");
            String elementIndent = indentString(indent + 1);
            String columnList = formatColumnList(node.getColumnDefines(), elementIndent);
            builder.append(columnList);
            if (!node.isPartitionEmpty()) {
                builder.append(",\n");
                String list = formatColumnList(node.getPartitionedBy().getColumnDefinitions(), elementIndent);
                builder.append(list);
            }
            if (!node.isConstraintEmpty()) {
                appendConstraint(node, indent);
            }
            if (!node.isIndexEmpty()) {
                Iterator<TableIndex> iterator = node.getTableIndexList().iterator();
                while (iterator.hasNext()) {
                    builder.append(",\n");
                    process(iterator.next(), indent + 1);
                }
            }
            builder.append("\n").append(")");
        } else {
            if (!node.isCommentElementEmpty()) {
                builder.append(newLine("/*("));
                String elementIndent = indentString(indent + 1);
                builder.append(formatCommentElement(node.getColumnCommentElements(), elementIndent));
                builder.append(newLine(")*/"));
            }
        }
        builder.append(formatComment(node.getComment()));
        return executable;
    }

    @Override
    protected String formatCommentElement(List<MultiComment> commentElements, String elementIndent) {
        return commentElements.stream().map(
            element -> {
                MysqlVisitor visitor = new MysqlVisitor(this.mysqlTransformContext);
                visitor.process(element.getNode(), 0);
                String result = visitor.getBuilder().toString();
                return elementIndent + result;
            }).collect(Collectors.joining(",\n"));
    }

    private void appendConstraint(CreateTable node, Integer indent) {
        Iterator<BaseConstraint> iterator = node.getConstraintStatements().iterator();
        while (iterator.hasNext()) {
            BaseConstraint next = iterator.next();
            if (next instanceof PrimaryConstraint || next instanceof UniqueConstraint) {
                builder.append(",\n");
                process(next, indent + 1);
            }
        }
    }

    @Override
    public Boolean visitPrimaryConstraint(PrimaryConstraint primaryConstraint, Integer indent) {
        builder.append(indentString(indent)).append("PRIMARY KEY(");
        builder.append(
            primaryConstraint.getColNames().stream().map(ExpressionFormatter::formatExpression).collect(joining(",")));
        builder.append(")");
        return true;
    }

    @Override
    public Boolean visitAddCols(AddCols addCols, Integer context) {
        // Check if this operation is already handled as part of a grouped operation
        if (!groupedAlterOperations.isEmpty() && groupedAlterOperations.containsKey(addCols.getQualifiedName())) {
            // If we're currently grouping operations and this table is in the grouping,
            // the operation will be handled together in processGroupedAlterTable
            return true;
        }

        builder.append("ALTER TABLE ").append(getCode(addCols.getQualifiedName()));
        builder.append(" ADD COLUMN\n").append('(').append("\n");
        String elementIndent = indentString(context + 1);
        String columnList = formatColumnList(addCols.getColumnDefineList(), elementIndent);
        builder.append(columnList);
        builder.append("\n").append(')');
        return true;
    }

    @Override
    public Boolean visitDropConstraint(DropConstraint dropConstraint, Integer context) {
        if (dropConstraint.getConstraintType() == null) {
            super.visitDropConstraint(dropConstraint, context);
            return false;
        }

        // Check if this operation is already handled as part of a grouped operation
        if (!groupedAlterOperations.isEmpty() && groupedAlterOperations.containsKey(dropConstraint.getQualifiedName())) {
            // If we're currently grouping operations and this table is in the grouping,
            // the operation will be handled together in processGroupedAlterTable
            return true;
        }

        ConstraintType constraintType = dropConstraint.getConstraintType();
        if (constraintType == ConstraintType.PRIMARY_KEY) {
            builder.append("ALTER TABLE ").append(getCode(dropConstraint.getQualifiedName()));
            builder.append(" DROP PRIMARY KEY");
        } else if (constraintType == ConstraintType.DIM_KEY) {
            builder.append("ALTER TABLE ").append(getCode(dropConstraint.getQualifiedName()));
            if (mysqlTransformContext.isGenerateForeignKey()) {
                builder.append(" DROP FOREIGN KEY ").append(formatExpression(dropConstraint.getConstraintName()));
            } else {
                super.visitDropConstraint(dropConstraint, context);
                return false;
            }
        } else {
            super.visitDropConstraint(dropConstraint, context);
            return false;
        }
        return true;
    }

    @Override
    public Boolean visitChangeCol(ChangeCol renameCol, Integer context) {
        // Check if this operation is already handled as part of a grouped operation
        if (!groupedAlterOperations.isEmpty() && groupedAlterOperations.containsKey(renameCol.getQualifiedName())) {
            // If we're currently grouping operations and this table is in the grouping,
            // the operation will be handled together in processGroupedAlterTable
            return true;
        }

        builder.append("ALTER TABLE ").append(getCode(renameCol.getQualifiedName()));
        builder.append(" CHANGE COLUMN ").append(ExpressionFormatter.formatExpression(renameCol.getOldColName()));
        builder.append(" ").append(formatColumnDefinition(renameCol.getColumnDefinition(), 0));
        return true;
    }

    @Override
    public Boolean visitAddConstraint(AddConstraint addConstraint, Integer context) {
        // Check if this operation is already handled as part of a grouped operation
        if (!groupedAlterOperations.isEmpty() && groupedAlterOperations.containsKey(addConstraint.getQualifiedName())) {
            // If we're currently grouping operations and this table is in the grouping,
            // the operation will be handled together in processGroupedAlterTable
            return true;
        }

        BaseConstraint constraintStatement = addConstraint.getConstraintStatement();
        if (constraintStatement instanceof PrimaryConstraint) {
            PrimaryConstraint constraint = (PrimaryConstraint)constraintStatement;
            List<Identifier> colNames = constraint.getColNames();
            builder.append("ALTER TABLE ").append(getCode(addConstraint.getQualifiedName()));
            builder.append(" ADD CONSTRAINT ").append(formatExpression(constraint.getName()));
            builder.append(" PRIMARY KEY ");
            builder.append("(");
            builder.append(constraint.getColNames().stream().map(Identifier::getValue).collect(joining(",")));
            builder.append(")");
        } else if (constraintStatement instanceof DimConstraint) {
            if (!mysqlTransformContext.isGenerateForeignKey()) {
                super.visitAddConstraint(addConstraint, context);
                return false;
            } else {
                DimConstraint dimConstraint = (DimConstraint)constraintStatement;
                List<Identifier> colNames = dimConstraint.getColNames();
                if (CollectionUtils.isEmpty(colNames)) {
                    super.visitAddConstraint(addConstraint, context);
                    return false;
                }
                builder.append("ALTER TABLE ").append(getCode(addConstraint.getQualifiedName()));
                builder.append(" ADD CONSTRAINT ").append(
                    formatExpression(addConstraint.getConstraintStatement().getName()));
                builder.append(" FOREIGN KEY (");

                builder.append(colNames.stream().map(Identifier::getValue).collect(joining(","))).append(")");
                List<Identifier> referenceColNames = dimConstraint.getReferenceColNames();
                builder.append(" REFERENCES ").append(dimConstraint.getReferenceTable()).append("(").append(
                    referenceColNames.stream().map(Identifier::getValue).collect(joining(","))).append(")");
            }
        } else {
            super.visitAddConstraint(addConstraint, context);
            return false;
        }
        return true;
    }

    @Override
    public Boolean visitDropCol(DropCol dropCol, Integer context) {
        // Check if this operation is already handled as part of a grouped operation
        if (!groupedAlterOperations.isEmpty() && groupedAlterOperations.containsKey(dropCol.getQualifiedName())) {
            // If we're currently grouping operations and this table is in the grouping,
            // the operation will be handled together in processGroupedAlterTable
            return true;
        }

        builder.append("ALTER TABLE ").append(getCode(dropCol.getQualifiedName()));
        builder.append(" DROP COLUMN ").append(formatExpression(dropCol.getColumnName()));
        return true;
    }

    @Override
    public Boolean visitRenameCol(RenameCol renameCol, Integer context) {
        // Check if this operation is already handled as part of a grouped operation
        if (!groupedAlterOperations.isEmpty() && groupedAlterOperations.containsKey(renameCol.getQualifiedName())) {
            // If we're currently grouping operations and this table is in the grouping,
            // the operation will be handled together in processGroupedAlterTable
            return true;
        }

        builder.append("ALTER TABLE ").append(getCode(renameCol.getQualifiedName()));
        builder.append(" RENAME COLUMN ").append(formatExpression(renameCol.getOldColName()))
            .append(" TO ").append(formatExpression(renameCol.getNewColName()));
        return true;
    }

    @Override
    public Boolean visitSetColComment(SetColComment setColComment, Integer context) {
        // Check if this operation is already handled as part of a grouped operation
        if (!groupedAlterOperations.isEmpty() && groupedAlterOperations.containsKey(setColComment.getQualifiedName())) {
            // If we're currently grouping operations and this table is in the grouping,
            // the operation will be handled together in processGroupedAlterTable
            return true;
        }

        builder.append("ALTER TABLE ").append(getCode(setColComment.getQualifiedName()));
        builder.append(" MODIFY COLUMN ").append(formatExpression(setColComment.getChangeColumn()))
            .append(" COMMENT ").append(formatStringLiteral(setColComment.getComment().getComment()));
        return true;
    }

    @Override
    public Boolean visitSetTableComment(SetTableComment setTableComment, Integer context) {
        // Check if this operation is already handled as part of a grouped operation
        if (!groupedAlterOperations.isEmpty() && groupedAlterOperations.containsKey(setTableComment.getQualifiedName())) {
            // If we're currently grouping operations and this table is in the grouping,
            // the operation will be handled together in processGroupedAlterTable
            return true;
        }

        builder.append("ALTER TABLE ").append(getCode(setTableComment.getQualifiedName()));
        builder.append(" COMMENT ").append(formatStringLiteral(setTableComment.getComment().getComment()));
        return true;
    }

    @Override
    public Boolean visitAddPartitionCol(AddPartitionCol addPartitionCol, Integer context) {
        super.visitAddPartitionCol(addPartitionCol, context);
        return false;
    }

    @Override
    public Boolean visitDropPartitionCol(DropPartitionCol dropPartitionCol, Integer context) {
        super.visitDropPartitionCol(dropPartitionCol, context);
        return false;
    }

    @Override
    public Boolean visitUnSetTableProperties(UnSetTableProperties unSetTableProperties, Integer context) {
        super.visitUnSetTableProperties(unSetTableProperties, context);
        return false;
    }

    @Override
    public Boolean visitSetTableProperties(SetTableProperties setTableProperties, Integer context) {
        super.visitSetTableProperties(setTableProperties, context);
        return false;
    }

    @Override
    protected String getCode(QualifiedName qualifiedName) {
        QualifiedName tableName = StringJoinUtil.join(this.mysqlTransformContext.getDatabase(),
            this.mysqlTransformContext.getSchema(), qualifiedName.getSuffix());
        return formatName(tableName);
    }

    @Override
    protected String formatColumnDefinition(ColumnDefinition column, Integer max) {
        BaseDataType dataType = column.getDataType();
        StringBuilder sb = new StringBuilder();
        sb.append(formatColName(column.getColName(), max));
        sb.append(" ").append(formatExpression(convert(dataType)));
        Boolean notNull = column.getNotNull();
        if (BooleanUtils.isTrue(notNull)) {
            sb.append(" NOT NULL");
        } else if (BooleanUtils.isFalse(notNull)) {
            sb.append(" NULL");
        }
        if (column.getDefaultValue() != null) {
            sb.append(" DEFAULT ").append(formatExpression(column.getDefaultValue()));
        }
        boolean isPrimary = column.getPrimary() != null && column.getPrimary();
        if (isPrimary) {
            if (mysqlTransformContext.isAutoIncrement() && DataTypeEnums.isIntDataType(dataType.getTypeName())) {
                sb.append(" AUTO_INCREMENT ");
            }
            List<Property> columnProperties = column.getColumnProperties();
            sb.append(" PRIMARY KEY");
        }
        sb.append(formatComment(column.getComment()));
        return sb.toString();
    }

    @Override
    protected BaseDataType convert(BaseDataType dataType) {
        if (dataTypeTransformer != null) {
            return dataTypeTransformer.convert(dataType);
        }
        IDataTypeName typeName = dataType.getTypeName();
        if (StringUtils.equalsIgnoreCase(typeName.getValue(), DataTypeEnums.STRING.getValue())) {
            return new GenericDataType(new Identifier(DataTypeEnums.VARCHAR.name()),
                ImmutableList.of(new NumericParameter(mysqlTransformContext.getVarcharLength().toString())));
        } else if (typeName.getDimension() == Dimension.MULTIPLE) {
            return new GenericDataType(new Identifier(DataTypeEnums.JSON.name()));
        } else if (StringUtils.equalsIgnoreCase(typeName.getValue(), DataTypeEnums.BOOLEAN.getValue())) {
            return new GenericDataType(new Identifier(DataTypeEnums.CHAR.name()),
                ImmutableList.of(new NumericParameter("1")));
        }
        return dataType;
    }

    @Override
    public Boolean visitCompositeStatement(CompositeStatement node, Integer context) {
        // Initialize and group ALTER TABLE operations by table name
        initializeAndGroupAlterOperations(node);

        // Process statements in the original order with appropriate separators
        processStatementsInOrder(node, context);

        // Clear the map after processing
        groupedAlterOperations.clear();
        return true;
    }

    /**
     * Groups ALTER TABLE operations by table name for efficient processing
     */
    private void initializeAndGroupAlterOperations(CompositeStatement node) {
        groupedAlterOperations.clear();

        for (BaseStatement stmt : node.getStatements()) {
            if (isAlterTableStatement(stmt)) {
                QualifiedName tableName = getTableNameFromStatement(stmt);
                if (tableName != null) {
                    groupedAlterOperations.computeIfAbsent(tableName, k -> new ArrayList<>()).add(stmt);
                }
            }
        }
    }

    /**
     * Processes statements in their original order with appropriate separators
     */
    private void processStatementsInOrder(CompositeStatement node, Integer context) {
        for (int i = 0; i < node.getStatements().size(); i++) {
            BaseStatement currentStatement = node.getStatements().get(i);
            boolean isCurrentAlter = isAlterTableStatement(currentStatement);

            if (isCurrentAlter) {
                processAlterStatement(currentStatement, i, node);
            } else {
                processNonAlterStatement(currentStatement, i, node, context);
            }
        }
    }

    /**
     * Processes an ALTER statement, grouping operations for the same table
     */
    private void processAlterStatement(BaseStatement currentStatement, int currentIndex, CompositeStatement node) {
        QualifiedName tableName = getTableNameFromStatement(currentStatement);

        // Only process if this is the first statement for this table group (to avoid reprocessing)
        if (tableName != null && groupedAlterOperations.containsKey(tableName)) {
            // Process the entire group for this table
            List<BaseStatement> group = groupedAlterOperations.get(tableName);
            processGroupedAlterTable(tableName, group, 0); // context is typically 0 for grouped operations

            // Add appropriate separator after the grouped operation
            addSeparatorAfterStatement(currentIndex, node, isAlterTableStatement(currentStatement));

            // Remove the group to prevent reprocessing
            groupedAlterOperations.remove(tableName);
        }
    }

    /**
     * Processes a non-ALTER statement
     */
    private void processNonAlterStatement(BaseStatement currentStatement, int currentIndex,
        CompositeStatement node, Integer context) {
        process(currentStatement, context);

        // Add appropriate separator after the statement
        addSeparatorAfterStatement(currentIndex, node, isAlterTableStatement(currentStatement));
    }

    /**
     * Adds separator after a statement based on the current and next statement types
     */
    private void addSeparatorAfterStatement(int currentIndex, CompositeStatement node, boolean isCurrentAlter) {
        if (currentIndex < node.getStatements().size() - 1) {
            // Not the last statement, add separator based on type transition
            boolean nextIsAlter = isAlterTableStatement(node.getStatements().get(currentIndex + 1));
            addSeparatorBasedOnTypeTransition(isCurrentAlter, nextIsAlter);
        } else {
            // Last statement, add semicolon only
            builder.append(";");
        }
    }

    /**
     * Adds appropriate separator based on transition between statement types
     */
    private void addSeparatorBasedOnTypeTransition(boolean isCurrentAlter, boolean isNextAlter) {
        if (isCurrentAlter != isNextAlter) {
            // Transition between different types (alter <-> non-alter): add semicolon and extra line break
            builder.append(";\n\n");
        } else {
            // Same type (alter <-> alter or non-alter <-> non-alter): add semicolon and single line break
            builder.append(";\n");
        }
    }

    /**
     * Checks if the statement is an ALTER TABLE statement
     */
    private boolean isAlterTableStatement(BaseStatement stmt) {
        return stmt instanceof AddCols
            || stmt instanceof ChangeCol
            || stmt instanceof DropCol
            || stmt instanceof AddConstraint
            || stmt instanceof DropConstraint
            || stmt instanceof RenameCol
            || stmt instanceof SetTableComment
            || stmt instanceof SetColComment;
    }

    /**
     * Extracts table name from an ALTER TABLE statement
     */
    private QualifiedName getTableNameFromStatement(BaseStatement stmt) {
        if (stmt instanceof BaseOperatorStatement) {
            return ((BaseOperatorStatement)stmt).getQualifiedName();
        }
        return null;
    }

    /**
     * Processes a group of ALTER TABLE operations for the same table
     */
    private void processGroupedAlterTable(QualifiedName tableName, List<BaseStatement> operations, Integer context) {
        builder.append("ALTER TABLE ").append(getCode(tableName)).append("\n");

        for (int i = 0; i < operations.size(); i++) {
            BaseStatement operation = operations.get(i);

            // Process the operation without the ALTER TABLE prefix
            if (operation instanceof AddCols) {
                processAddColsWithoutPrefix((AddCols)operation, context);
            } else if (operation instanceof ChangeCol) {
                processChangeColWithoutPrefix((ChangeCol)operation, context);
            } else if (operation instanceof DropCol) {
                processDropColWithoutPrefix((DropCol)operation, context);
            } else if (operation instanceof AddConstraint) {
                processAddConstraintWithoutPrefix((AddConstraint)operation, context);
            } else if (operation instanceof DropConstraint) {
                processDropConstraintWithoutPrefix((DropConstraint)operation, context);
            } else if (operation instanceof RenameCol) {
                processRenameColWithoutPrefix((RenameCol)operation, context);
            } else if (operation instanceof SetColComment) {
                processSetColCommentWithoutPrefix((SetColComment)operation, context);
            } else if (operation instanceof SetTableComment) {
                processSetTableCommentWithoutPrefix((SetTableComment)operation, context);
            }

            if (i < operations.size() - 1) {
                builder.append(",\n");
            }
        }
    }

    /**
     * Process AddCols without ALTER TABLE prefix
     * For grouped operations, each column definition should be its own ADD COLUMN clause
     */
    private void processAddColsWithoutPrefix(AddCols addCols, Integer context) {
        List<ColumnDefinition> columns = addCols.getColumnDefineList();
        for (int i = 0; i < columns.size(); i++) {
            ColumnDefinition column = columns.get(i);
            builder.append("  ADD COLUMN ").append(formatColumnDefinition(column, 0));
            if (i < columns.size() - 1) {
                builder.append(",\n");
            }
        }
    }

    /**
     * Process ChangeCol without ALTER TABLE prefix
     */
    private void processChangeColWithoutPrefix(ChangeCol changeCol, Integer context) {
        builder.append("  CHANGE COLUMN ").append(ExpressionFormatter.formatExpression(changeCol.getOldColName()));
        builder.append(" ").append(formatColumnDefinition(changeCol.getColumnDefinition(), 0));
    }

    /**
     * Process DropCol without ALTER TABLE prefix
     */
    private void processDropColWithoutPrefix(DropCol dropCol, Integer context) {
        builder.append("  DROP COLUMN ").append(formatExpression(dropCol.getColumnName()));
    }

    /**
     * Process AddConstraint without ALTER TABLE prefix
     */
    private void processAddConstraintWithoutPrefix(AddConstraint addConstraint, Integer context) {
        builder.append("  ADD ");
        process(addConstraint.getConstraintStatement(), 0); // Use 0 as context for constraint to avoid indentation issue
    }

    /**
     * Process DropConstraint without ALTER TABLE prefix
     */
    private void processDropConstraintWithoutPrefix(DropConstraint dropConstraint, Integer context) {
        ConstraintType constraintType = dropConstraint.getConstraintType();
        if (constraintType == ConstraintType.PRIMARY_KEY) {
            builder.append("  DROP PRIMARY KEY");
        } else if (constraintType == ConstraintType.DIM_KEY) {
            if (mysqlTransformContext.isGenerateForeignKey()) {
                builder.append("  DROP FOREIGN KEY ").append(formatExpression(dropConstraint.getConstraintName()));
            } else {
                builder.append("  DROP CONSTRAINT ").append(formatExpression(dropConstraint.getConstraintName()));
            }
        } else {
            builder.append("  DROP CONSTRAINT ").append(formatExpression(dropConstraint.getConstraintName()));
        }
    }

    /**
     * Process RenameCol without ALTER TABLE prefix
     */
    private void processRenameColWithoutPrefix(RenameCol renameCol, Integer context) {
        builder.append("  RENAME COLUMN ").append(formatExpression(renameCol.getOldColName()))
            .append(" TO ").append(formatExpression(renameCol.getNewColName()));
    }

    /**
     * Process SetColComment without ALTER TABLE prefix
     */
    private void processSetColCommentWithoutPrefix(SetColComment setColComment, Integer context) {
        builder.append("  MODIFY COLUMN ").append(formatExpression(setColComment.getChangeColumn()))
            .append(" COMMENT ").append(formatStringLiteral(setColComment.getComment().getComment()));
    }

    /**
     * Process SetTableComment without ALTER TABLE prefix
     */
    private void processSetTableCommentWithoutPrefix(SetTableComment setTableComment, Integer context) {
        builder.append("  COMMENT ").append(formatStringLiteral(setTableComment.getComment().getComment()));
    }

    /**
     * Process grouped alter table operations with proper semicolon handling
     */
    private void processGroupedAlterTableWithSemicolon(QualifiedName tableName, List<BaseStatement> operations, Integer context) {
        builder.append("ALTER TABLE ").append(getCode(tableName)).append("\n");

        for (int i = 0; i < operations.size(); i++) {
            BaseStatement operation = operations.get(i);

            // Process the operation without the ALTER TABLE prefix
            if (operation instanceof AddCols) {
                processAddColsWithoutPrefix((AddCols)operation, context);
            } else if (operation instanceof ChangeCol) {
                processChangeColWithoutPrefix((ChangeCol)operation, context);
            } else if (operation instanceof DropCol) {
                processDropColWithoutPrefix((DropCol)operation, context);
            } else if (operation instanceof AddConstraint) {
                processAddConstraintWithoutPrefix((AddConstraint)operation, context);
            } else if (operation instanceof DropConstraint) {
                processDropConstraintWithoutPrefix((DropConstraint)operation, context);
            } else if (operation instanceof RenameCol) {
                processRenameColWithoutPrefix((RenameCol)operation, context);
            } else if (operation instanceof SetColComment) {
                processSetColCommentWithoutPrefix((SetColComment)operation, context);
            } else if (operation instanceof SetTableComment) {
                processSetTableCommentWithoutPrefix((SetTableComment)operation, context);
            }

            if (i < operations.size() - 1) {
                builder.append(",\n");
            }
        }
    }

    @Override
    public Boolean visitRefEntityStatement(RefRelation refEntityStatement, Integer context) {
        // 转为dim constraint
        RefObject left = refEntityStatement.getLeft();
        RefObject right = refEntityStatement.getRight();
        List<Identifier> columnList = left.getAttrNameList();
        List<Identifier> rightColumnList = right.getAttrNameList();
        if (CollectionUtils.isEmpty(columnList) || CollectionUtils.isEmpty(rightColumnList)) {
            return false;
        }
        // ALTER TABLE `a` ADD CONSTRAINT `name` FOREIGN KEY (`a`) REFERENCES `b` (`a`);
        builder.append("ALTER TABLE ").append(getCode(left.getMainName()));
        builder.append(" ADD CONSTRAINT ").append(formatName(refEntityStatement.getQualifiedName()));
        String collect = columnList.stream().map(this::formatExpression).collect(joining(","));
        builder.append(" FOREIGN KEY (").append(collect).append(")");
        builder.append(" REFERENCES ").append(formatName(right.getMainName()));
        String rightReference = rightColumnList.stream().map(this::formatExpression).collect(joining(","));
        builder.append(" (").append(rightReference).append(")");
        return true;
    }

    @Override
    protected String formatExpression(BaseExpression baseExpression) {
        return new DefaultExpressionVisitor().process(baseExpression);
    }
}
