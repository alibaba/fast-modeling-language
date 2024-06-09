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

package com.aliyun.fastmodel.core.formatter;

import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.core.tree.AliasedName;
import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.BaseRelation;
import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.ListNode;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.literal.StringLiteral;
import com.aliyun.fastmodel.core.tree.relation.AliasedRelation;
import com.aliyun.fastmodel.core.tree.relation.Join;
import com.aliyun.fastmodel.core.tree.relation.SampledRelation;
import com.aliyun.fastmodel.core.tree.relation.join.JoinCriteria;
import com.aliyun.fastmodel.core.tree.relation.join.JoinOn;
import com.aliyun.fastmodel.core.tree.relation.join.JoinToken;
import com.aliyun.fastmodel.core.tree.relation.join.JoinUsing;
import com.aliyun.fastmodel.core.tree.relation.querybody.Except;
import com.aliyun.fastmodel.core.tree.relation.querybody.Intersect;
import com.aliyun.fastmodel.core.tree.relation.querybody.QuerySpecification;
import com.aliyun.fastmodel.core.tree.relation.querybody.Table;
import com.aliyun.fastmodel.core.tree.relation.querybody.TableSubQuery;
import com.aliyun.fastmodel.core.tree.relation.querybody.Union;
import com.aliyun.fastmodel.core.tree.relation.querybody.Values;
import com.aliyun.fastmodel.core.tree.statement.BaseCreate;
import com.aliyun.fastmodel.core.tree.statement.BaseDrop;
import com.aliyun.fastmodel.core.tree.statement.BaseRename;
import com.aliyun.fastmodel.core.tree.statement.BaseSetAliasedName;
import com.aliyun.fastmodel.core.tree.statement.BaseSetComment;
import com.aliyun.fastmodel.core.tree.statement.BaseSetProperties;
import com.aliyun.fastmodel.core.tree.statement.BaseUnSetProperties;
import com.aliyun.fastmodel.core.tree.statement.CompositeStatement;
import com.aliyun.fastmodel.core.tree.statement.adjunct.CreateAdjunct;
import com.aliyun.fastmodel.core.tree.statement.constants.ColumnCategory;
import com.aliyun.fastmodel.core.tree.statement.constants.ConstraintType;
import com.aliyun.fastmodel.core.tree.statement.constants.TableDetailType;
import com.aliyun.fastmodel.core.tree.statement.desc.Describe;
import com.aliyun.fastmodel.core.tree.statement.dict.CreateDict;
import com.aliyun.fastmodel.core.tree.statement.dimension.AddDimensionAttribute;
import com.aliyun.fastmodel.core.tree.statement.dimension.ChangeDimensionAttribute;
import com.aliyun.fastmodel.core.tree.statement.dimension.CreateDimension;
import com.aliyun.fastmodel.core.tree.statement.dimension.DropDimensionAttribute;
import com.aliyun.fastmodel.core.tree.statement.dimension.attribute.AttributeCategory;
import com.aliyun.fastmodel.core.tree.statement.dimension.attribute.DimensionAttribute;
import com.aliyun.fastmodel.core.tree.statement.dqc.AddDqcRule;
import com.aliyun.fastmodel.core.tree.statement.dqc.ChangeDqcRule;
import com.aliyun.fastmodel.core.tree.statement.dqc.ChangeDqcRuleElement;
import com.aliyun.fastmodel.core.tree.statement.dqc.CreateDqcRule;
import com.aliyun.fastmodel.core.tree.statement.dqc.DropDqcRule;
import com.aliyun.fastmodel.core.tree.statement.dqc.check.BaseCheckElement;
import com.aliyun.fastmodel.core.tree.statement.element.MultiComment;
import com.aliyun.fastmodel.core.tree.statement.group.CreateGroup;
import com.aliyun.fastmodel.core.tree.statement.indicator.CreateAtomicCompositeIndicator;
import com.aliyun.fastmodel.core.tree.statement.indicator.CreateAtomicIndicator;
import com.aliyun.fastmodel.core.tree.statement.indicator.CreateDerivativeCompositeIndicator;
import com.aliyun.fastmodel.core.tree.statement.indicator.CreateDerivativeIndicator;
import com.aliyun.fastmodel.core.tree.statement.indicator.SetIndicatorProperties;
import com.aliyun.fastmodel.core.tree.statement.insert.Insert;
import com.aliyun.fastmodel.core.tree.statement.layer.Checker;
import com.aliyun.fastmodel.core.tree.statement.layer.CreateLayer;
import com.aliyun.fastmodel.core.tree.statement.references.MoveReferences;
import com.aliyun.fastmodel.core.tree.statement.rule.AddRules;
import com.aliyun.fastmodel.core.tree.statement.rule.ChangeRuleElement;
import com.aliyun.fastmodel.core.tree.statement.rule.ChangeRules;
import com.aliyun.fastmodel.core.tree.statement.rule.CreateRules;
import com.aliyun.fastmodel.core.tree.statement.rule.DropRule;
import com.aliyun.fastmodel.core.tree.statement.rule.PartitionSpec;
import com.aliyun.fastmodel.core.tree.statement.rule.RuleDefinition;
import com.aliyun.fastmodel.core.tree.statement.rule.strategy.DynamicStrategy;
import com.aliyun.fastmodel.core.tree.statement.rule.strategy.FixedStrategy;
import com.aliyun.fastmodel.core.tree.statement.rule.strategy.VolStrategy;
import com.aliyun.fastmodel.core.tree.statement.script.ImportObject;
import com.aliyun.fastmodel.core.tree.statement.script.RefObject;
import com.aliyun.fastmodel.core.tree.statement.script.RefRelation;
import com.aliyun.fastmodel.core.tree.statement.select.Limit;
import com.aliyun.fastmodel.core.tree.statement.select.Query;
import com.aliyun.fastmodel.core.tree.statement.select.Select;
import com.aliyun.fastmodel.core.tree.statement.select.With;
import com.aliyun.fastmodel.core.tree.statement.select.WithQuery;
import com.aliyun.fastmodel.core.tree.statement.select.groupby.Cube;
import com.aliyun.fastmodel.core.tree.statement.select.groupby.GroupingElement;
import com.aliyun.fastmodel.core.tree.statement.select.groupby.GroupingSets;
import com.aliyun.fastmodel.core.tree.statement.select.groupby.Rollup;
import com.aliyun.fastmodel.core.tree.statement.select.groupby.SimpleGroupBy;
import com.aliyun.fastmodel.core.tree.statement.select.item.AllColumns;
import com.aliyun.fastmodel.core.tree.statement.select.item.SelectItem;
import com.aliyun.fastmodel.core.tree.statement.select.item.SingleColumn;
import com.aliyun.fastmodel.core.tree.statement.select.order.OrderBy;
import com.aliyun.fastmodel.core.tree.statement.show.LikeCondition;
import com.aliyun.fastmodel.core.tree.statement.show.ShowObjects;
import com.aliyun.fastmodel.core.tree.statement.show.WhereCondition;
import com.aliyun.fastmodel.core.tree.statement.table.AddCols;
import com.aliyun.fastmodel.core.tree.statement.table.AddConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.AddPartitionCol;
import com.aliyun.fastmodel.core.tree.statement.table.ChangeCol;
import com.aliyun.fastmodel.core.tree.statement.table.CloneTable;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateIndex;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.DropCol;
import com.aliyun.fastmodel.core.tree.statement.table.DropConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.DropIndex;
import com.aliyun.fastmodel.core.tree.statement.table.DropPartitionCol;
import com.aliyun.fastmodel.core.tree.statement.table.DropTable;
import com.aliyun.fastmodel.core.tree.statement.table.PartitionedBy;
import com.aliyun.fastmodel.core.tree.statement.table.RenameCol;
import com.aliyun.fastmodel.core.tree.statement.table.RenameTable;
import com.aliyun.fastmodel.core.tree.statement.table.SetColComment;
import com.aliyun.fastmodel.core.tree.statement.table.SetColumnOrder;
import com.aliyun.fastmodel.core.tree.statement.table.SetTableComment;
import com.aliyun.fastmodel.core.tree.statement.table.SetTableProperties;
import com.aliyun.fastmodel.core.tree.statement.table.UnSetTableProperties;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.BaseConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.ColumnGroupConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.DimConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.LevelConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.LevelDefine;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.PrimaryConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.RedundantConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.TimePeriodConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.UniqueConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.index.IndexColumnName;
import com.aliyun.fastmodel.core.tree.statement.table.index.SortType;
import com.aliyun.fastmodel.core.tree.statement.table.index.TableIndex;
import com.aliyun.fastmodel.core.tree.statement.table.type.ITableDetailType;
import com.aliyun.fastmodel.core.tree.util.IdentifierUtil;
import com.aliyun.fastmodel.core.tree.util.RuleUtil;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import lombok.Getter;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;

import static com.aliyun.fastmodel.core.tree.relation.join.JoinToken.CROSS;
import static com.aliyun.fastmodel.core.tree.relation.join.JoinToken.IMPLICIT;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

/**
 * FML的visitor的信息
 *
 * @author panguanjing
 * @date 2020/12/14
 */
public class FastModelVisitor extends AstVisitor<Boolean, Integer> {

    protected static final String INDENT = "   ";
    public static final String CONSTRAINT = "CONSTRAINT ";
    public static final String SUFFIX = ";";
    public static final String NEW_LINE = "\n";
    public static final String SUFFIX_NEW_LINE = ";\n";
    public static final String COMMENT = "--";

    @Getter
    protected StringBuilder builder = new StringBuilder();

    @Override
    public Boolean visitNode(Node node, Integer indent) {
        throw new UnsupportedOperationException("not yet implemented: " + node.getClass());
    }

    @Override
    public Boolean visitCompositeStatement(CompositeStatement compositeStatement, Integer context) {
        List<BaseStatement> statements = compositeStatement.getStatements();
        int size = statements.size();
        if (size == 1) {
            return process(statements.get(0), context);
        }
        for (int i = 0; i < size; i++) {
            BaseStatement node = statements.get(i);
            if (node == null) {
                continue;
            }
            Boolean process = process(node, context);
            String str = builder.toString();
            if (BooleanUtils.isNotTrue(process)) {
                String[] split = StringUtils.split(str, NEW_LINE);
                String content = split[split.length - 1];
                builder.insert(str.length() - content.length(), "-- ");
            }
            boolean endWithComma = str.endsWith(SUFFIX);
            if (i < size - 1) {
                if (endWithComma) {
                    builder.append(NEW_LINE);
                } else {
                    builder.append(SUFFIX_NEW_LINE);
                }
            } else {
                if (!endWithComma) {
                    builder.append(SUFFIX);
                }
            }

        }
        return true;
    }

    @Override
    public Boolean visitCreateAtomicIndicator(CreateAtomicIndicator createIndicator, Integer context) {
        appendCreateOrReplace(createIndicator);
        builder.append("ATOMIC INDICATOR ");
        appendIfNotExist(createIndicator.isNotExists());
        builder.append(getCode(createIndicator.getQualifiedName()));
        builder.append(formatAliasedName(createIndicator.getAliasedName()));
        builder.append(" ").append(createIndicator.getDataType());
        builder.append(formatComment(createIndicator.getComment()));
        appendProperties(builder, createIndicator.getProperties());
        appendExpression(createIndicator.getIndicatorExpr());
        return true;
    }

    private void appendCreateOrReplace(BaseCreate baseCreate) {
        builder.append("CREATE ");
        if (BooleanUtils.isTrue(baseCreate.getCreateElement().getCreateOrReplace())) {
            builder.append("OR REPLACE ");
        }
    }

    @Override
    public Boolean createAtomicCompositeIndicator(CreateAtomicCompositeIndicator createIndicator, Integer context) {
        appendCreateOrReplace(createIndicator);
        builder.append("ATOMIC COMPOSITE INDICATOR ");
        appendIfNotExist(createIndicator.isNotExists());
        builder.append(getCode(createIndicator.getQualifiedName()));
        builder.append(formatAliasedName(createIndicator.getAliasedName()));
        builder.append(" ").append(createIndicator.getDataType());
        builder.append(formatComment(createIndicator.getComment()));
        appendProperties(builder, createIndicator.getProperties());
        appendExpression(createIndicator.getIndicatorExpr());
        return true;
    }

    /**
     * Get Code
     *
     * @param qualifiedName
     * @return 返回的code内容
     */
    protected String getCode(QualifiedName qualifiedName) {
        return formatName(qualifiedName);
    }

    @Override
    public Boolean visitCreateDerivativeCompositeIndicator(
        CreateDerivativeCompositeIndicator createIndicator, Integer context) {
        appendCreateOrReplace(createIndicator);
        builder.append("DERIVATIVE COMPOSITE INDICATOR ");
        appendIfNotExist(createIndicator.isNotExists());
        builder.append(getCode(createIndicator.getQualifiedName()));
        builder.append(formatAliasedName(createIndicator.getAliasedName()));
        if (createIndicator.getDataType() != null) {
            builder.append(" ").append(createIndicator.getDataType());
        }
        appendReference(createIndicator.getReferences());
        builder.append(formatComment(createIndicator.getComment()));
        appendProperties(builder, createIndicator.getProperties());
        appendExpression(createIndicator.getIndicatorExpr());
        return true;
    }

    @Override
    public Boolean visitCreateDerivativeIndicator(CreateDerivativeIndicator createIndicator, Integer context) {
        appendCreateOrReplace(createIndicator);
        builder.append("DERIVATIVE INDICATOR ");
        appendIfNotExist(createIndicator.isNotExists());
        builder.append(getCode(createIndicator.getQualifiedName()));
        builder.append(formatAliasedName(createIndicator.getAliasedName()));
        if (createIndicator.getDataType() != null) {
            builder.append(" ").append(createIndicator.getDataType());
        }
        appendReference(createIndicator.getReferences());
        builder.append(formatComment(createIndicator.getComment()));
        appendProperties(builder, createIndicator.getProperties());
        appendExpression(createIndicator.getIndicatorExpr());
        return true;
    }

    private void appendExpression(BaseExpression baseExpression) {
        if (baseExpression != null) {
            builder.append("\n AS ");
            builder.append(formatExpression(baseExpression));
        }
    }

    private void appendReference(QualifiedName references) {
        if (references != null) {
            builder.append(" REFERENCES ").append(getCode(references));
        }
    }

    private void appendIfNotExist(boolean ifNotExist) {
        if (ifNotExist) {
            builder.append("IF NOT EXISTS ");
        }
    }

    @Override
    public Boolean visitCreateLayer(CreateLayer createLayer, Integer context) {
        builder.append("CREATE LAYER ");
        if (createLayer.isNotExists()) {
            builder.append("IF NOT EXISTS ");
        }
        builder.append(getCode(createLayer.getQualifiedName()));
        builder.append(formatAliasedName(createLayer.getAliasedName()));
        List<Checker> checkers = createLayer.getCheckers();
        if (checkers != null && !checkers.isEmpty()) {
            builder.append("(");
            String collect = checkers.stream().map(this::formatChecker).collect(joining(",\n"));
            builder.append(collect);
            builder.append(")");
        }
        builder.append(formatComment(createLayer.getComment()));
        appendProperties(builder, createLayer.getProperties());
        return true;
    }

    private String formatChecker(Checker checker) {
        return " CHECKER " + checker.getCheckerType().getCode() + " " + formatExpression(checker.getCheckerName()) + " "
            +
            formatExpression(checker.getExpression()) + formatComment(checker.getComment());
    }

    private void appendProperties(StringBuilder sb, List<Property> properties) {
        if (properties == null || properties.isEmpty()) {
            return;
        }
        String formatProperty = formatProperty(properties);
        if (Strings.isNullOrEmpty(formatProperty)) {
            return;
        }
        sb.append(" WITH (");
        sb.append(formatProperty);
        sb.append(")");
    }

    protected String formatComment(Comment comment) {
        return formatComment(comment, false);
    }

    protected String formatComment(Comment comment, boolean newLine) {
        if (comment == null) {
            return StringUtils.EMPTY;
        }
        if (comment.getComment() == null) {
            return StringUtils.EMPTY;
        }
        if (newLine) {
            return "COMMENT " + formatStringLiteral(comment.getComment());
        } else {
            return " COMMENT " + formatStringLiteral(comment.getComment());
        }
    }

    private String formatAliasedName(AliasedName aliasedName) {
        if (aliasedName == null) {
            return StringUtils.EMPTY;
        }
        if (aliasedName.getName() == null) {
            return StringUtils.EMPTY;
        }
        return " ALIAS " + formatStringLiteral(aliasedName.getName());
    }

    @Override
    public Boolean visitDropTable(DropTable dropTable, Integer context) {
        builder.append("DROP TABLE ");
        if (dropTable.isExists()) {
            builder.append("IF EXISTS ");
        }
        builder.append(getCode(dropTable.getQualifiedName()));
        return true;
    }

    @Override
    public Boolean visitRenameTable(RenameTable renameTable, Integer context) {
        builder.append("ALTER TABLE ");
        builder.append(getCode(renameTable.getQualifiedName()));
        builder.append(" RENAME TO ").append(getCode(renameTable.getTarget()));
        return true;
    }

    @Override
    public Boolean visitCreateTable(CreateTable node, Integer indent) {
        builder.append("CREATE");
        if (node.getCreateOrReplace() != null && node.getCreateOrReplace()) {
            builder.append(" OR REPLACE");
        }
        ITableDetailType tableDetailType = null;
        if (node.getTableDetailType() != null) {
            tableDetailType = node.getTableDetailType();
            boolean isNormalOrTransaction = tableDetailType == TableDetailType.NORMAL_DIM
                || tableDetailType == TableDetailType.TRANSACTION_FACT;
            if (!isNormalOrTransaction && !tableDetailType.isSingle()) {
                builder.append(" ").append(tableDetailType.getCode());
            }
            builder.append(" ").append(tableDetailType.getParent());
        }
        builder.append(" TABLE ");
        if (node.isNotExists()) {
            builder.append("IF NOT EXISTS ");
        }
        String tableName = getCode(node.getQualifiedName());
        builder.append(tableName);
        builder.append(formatAliasedName(node.getAliasedName()));
        int newIndent = indent + 1;
        if (!node.isColumnEmpty()) {
            builder.append(" \n(\n");
            String elementIndent = indentString(newIndent);
            builder.append(formatColumnList(node.getColumnDefines(), elementIndent));
            if (!node.isConstraintEmpty()) {
                Iterator<BaseConstraint> iterator = node.getConstraintStatements().iterator();
                while (iterator.hasNext()) {
                    builder.append(",\n");
                    process(iterator.next(), newIndent);
                }
            }
            if (!node.isIndexEmpty()) {
                Iterator<TableIndex> iterator = node.getTableIndexList().iterator();
                while (iterator.hasNext()) {
                    builder.append(",\n");
                    process(iterator.next(), newIndent);
                }
            }
            builder.append(newLine(")"));
        } else {
            if (!node.isCommentElementEmpty()) {
                builder.append(newLine("/*("));
                String elementIndent = indentString(newIndent);
                builder.append(formatCommentElement(node.getColumnCommentElements(), elementIndent));
                builder.append(newLine(")*/"));
            }
        }
        builder.append(formatComment(node.getComment(), isEndNewLine(builder.toString())));
        if (!node.isPartitionEmpty()) {
            PartitionedBy partitionedBy = node.getPartitionedBy();
            List<ColumnDefinition> columnDefinitions = partitionedBy.getColumnDefinitions();
            builder.append(
                formatPartitions(columnDefinitions, isEndNewLine(builder.toString()), indentString(newIndent)));
        }
        List<Property> properties = node.getProperties();
        builder.append(formatWith(properties, isEndNewLine(builder.toString())));
        removeNewLine(builder);
        return true;
    }

    @Override
    public Boolean visitMultiComment(MultiComment multiComment, Integer context) {
        builder.append("/*\n");
        builder.append(formatCommentElement(ImmutableList.of(multiComment), indentString(context + 1)));
        builder.append("\n*/");
        return true;
    }

    /**
     * 支持在列中增加注释信息
     *
     * @param commentElements
     * @param elementIndent
     * @return
     */
    protected String formatCommentElement(List<MultiComment> commentElements, String elementIndent) {
        return commentElements.stream().map(
            element -> {
                FastModelVisitor visitor = new FastModelVisitor();
                visitor.process(element.getNode(), 0);
                String result = visitor.getBuilder().toString();
                return elementIndent + result;
            }).collect(Collectors.joining(",\n"));
    }

    @Override
    public Boolean visitColumnDefine(ColumnDefinition columnDefine, Integer context) {
        builder.append(formatColumnDefinition(columnDefine, 0));
        return true;
    }

    @Override
    public Boolean visitDimConstraint(DimConstraint dimConstraint, Integer indent) {
        builder.append(indentString(indent));
        if (dimConstraint.getName() != null && !IdentifierUtil.isSysIdentifier(dimConstraint.getName())) {
            builder.append("CONSTRAINT ").append(formatExpression(dimConstraint.getName()));
        }
        List<Identifier> colNames = dimConstraint.getColNames();
        if (colNames != null) {
            builder.append(" DIM KEY (");
            String collect = colNames.stream().map(ExpressionFormatter::formatExpression).collect(joining(","));
            builder.append(collect).append(")");
        }
        builder.append(" REFERENCES ").append(dimConstraint.getReferenceTable());
        if (dimConstraint.getReferenceColNames() != null) {
            builder.append(" (").append(
                dimConstraint.getReferenceColNames().stream().map(ExpressionFormatter::formatExpression)
                    .collect(joining(","))).append(")");
        }
        return true;
    }

    @Override
    public Boolean visitUniqueConstraint(UniqueConstraint uniqueConstraint, Integer indent) {
        builder.append(indentString(indent));
        if (uniqueConstraint.getName() != null && !IdentifierUtil.isSysIdentifier(uniqueConstraint.getName())) {
            builder.append("CONSTRAINT ").append(formatExpression(uniqueConstraint.getName())).append(" ");
        }
        List<Identifier> colNames = uniqueConstraint.getColumnNames();
        builder.append("UNIQUE KEY (");
        String collect = colNames.stream().map(ExpressionFormatter::formatExpression).collect(joining(","));
        builder.append(collect).append(")");
        return true;
    }

    @Override
    public Boolean visitLevelConstraint(LevelConstraint levelConstraint, Integer indent) {
        builder.append(indentString(indent));
        Identifier name = levelConstraint.getName();
        if (name != null && !IdentifierUtil.isSysIdentifier(name)) {
            builder.append("CONSTRAINT ").append(formatExpression(levelConstraint.getName())).append(" ");
        }
        builder.append("LEVEL ");
        List<LevelDefine> levelDefines = levelConstraint.getLevelDefines();
        builder.append("<");
        builder.append(levelDefines.stream().map(this::formatLevelDefine).collect(joining(",")));
        builder.append(">");
        return true;
    }

    @Override
    public Boolean visitValues(Values values, Integer context) {
        builder.append(" VALUES ");

        boolean first = true;
        for (BaseExpression row : values.getRows()) {
            builder.append("\n")
                .append(indentString(context))
                .append(first ? "  " : ", ");
            builder.append(formatExpression(row));
            first = false;
        }
        builder.append('\n');
        return true;
    }

    private String formatLevelDefine(LevelDefine x) {
        StringBuilder sb = new StringBuilder();
        sb.append(ExpressionFormatter.formatExpression(x.getLevelColName()));
        if (x.isLevelPropColNameEmpty()) {
            return sb.toString();
        }
        sb.append(":").append("(");
        sb.append(x.getLevelPropColNames().stream().map(c -> ExpressionFormatter.formatExpression(c))
            .collect(joining(",")));
        sb.append(")");
        return sb.toString();
    }

    @Override
    public Boolean visitColumnGroupConstraint(ColumnGroupConstraint columnGroupConstraint, Integer indent) {
        builder.append(indentString(indent));
        if (columnGroupConstraint.getName() != null) {
            builder.append("CONSTRAINT ").append(formatExpression(columnGroupConstraint.getName()));
            builder.append(" ");
        }
        builder.append(ConstraintType.COLUMN_GROUP.getCode().toUpperCase()).append(" (").append(
            columnGroupConstraint.getColNames().stream().map(ExpressionFormatter::formatExpression)
                .collect(Collectors.joining(","))).append(")");
        return true;
    }

    @Override
    public Boolean visitPrimaryConstraint(PrimaryConstraint primaryConstraint, Integer ident) {
        Identifier name = primaryConstraint.getName();
        if (!IdentifierUtil.isSysIdentifier(name)) {
            builder.append(indentString(ident)).append(CONSTRAINT).append(formatExpression(name));
            builder.append(" PRIMARY KEY(");
        } else {
            builder.append(indentString(ident)).append("PRIMARY KEY(");
        }
        builder.append(
            primaryConstraint.getColNames().stream().map(this::formatExpression).collect(joining(",")));
        builder.append(")");
        if (primaryConstraint.getEnable() != null) {
            if (!primaryConstraint.getEnable()) {
                builder.append(" DISABLE");
            }
        }
        return true;
    }

    @Override
    public Boolean visitTimePeriodConstraint(TimePeriodConstraint timePeriodConstraint, Integer ident) {
        builder.append(indentString(ident));
        Identifier name = timePeriodConstraint.getName();
        if (!IdentifierUtil.isSysIdentifier(name)) {
            builder.append(CONSTRAINT).append(formatExpression(name));
            builder.append(" TIME_PERIOD KEY ");
        } else {
            builder.append(indentString(ident)).append("TIME_PERIOD KEY ");
        }
        builder.append("REFERENCES").append(" (");
        builder.append(timePeriodConstraint.getTimePeriods().stream().map(this::formatExpression).collect(joining(",")));
        builder.append(")");
        return true;
    }

    @Override
    public Boolean visitRedundantConstraint(RedundantConstraint redundantConstraint, Integer ident) {
        builder.append(indentString(ident));
        Identifier name = redundantConstraint.getName();
        String redunct = ConstraintType.REDUNDANT.getCode().toUpperCase();
        if (!IdentifierUtil.isSysIdentifier(name)) {
            builder.append(CONSTRAINT).append(formatExpression(name));
            builder.append(" ").append(redunct).append(" ");
        } else {
            builder.append(indentString(ident)).append(redunct).append(" ");
        }
        builder.append(formatExpression(redundantConstraint.getColumn()));
        builder.append(" REFERENCES ");
        builder.append(formatName(redundantConstraint.getJoinColumn()));
        List<Identifier> redundantColumns = redundantConstraint.getRedundantColumns();
        if (redundantColumns != null && !redundantColumns.isEmpty()) {
            builder.append("(");
            builder.append(redundantColumns.stream().map(this::formatExpression).collect(joining(",")));
            builder.append(")");
        }
        return false;
    }

    @Override
    public Boolean visitTableIndex(TableIndex tableIndex, Integer ident) {
        builder.append(indentString(ident));
        builder.append("INDEX ").append(formatExpression(tableIndex.getIndexName()));
        appendTableIndex(tableIndex.getIndexColumnNames());
        appendProperties(builder, tableIndex.getProperties());
        return true;
    }

    protected void appendTableIndex(List<IndexColumnName> tableIndex) {
        builder.append(" (");
        builder.append(tableIndex.stream().map(
            this::formatIndexColumnName
        ).collect(joining(",")));
        builder.append(")");
    }

    @Override
    public Boolean visitDropIndex(DropIndex dropIndex, Integer context) {
        builder.append("DROP INDEX ");
        builder.append(getCode(dropIndex.getQualifiedName()));
        builder.append(" ON ");
        builder.append(formatName(dropIndex.getTableName()));
        return true;
    }

    @Override
    public Boolean visitCreateIndex(CreateIndex createIndex, Integer context) {
        builder.append("CREATE INDEX ");
        builder.append(getCode(createIndex.getQualifiedName()));
        builder.append(" ON ");
        builder.append(formatName(createIndex.getTableName()));
        appendTableIndex(createIndex.getIndexColumnNameList());
        appendProperties(builder, createIndex.getPropertyList());
        return true;
    }

    private String formatIndexColumnName(IndexColumnName indexColumnName) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(formatExpression(indexColumnName.getColumnName()));
        if (indexColumnName.getColumnLength() != null) {
            stringBuilder.append("(").append(indexColumnName.getColumnLength().getValue()).append(")");
        }
        SortType sortType = indexColumnName.getSortType();
        if (sortType != null) {
            stringBuilder.append(" ").append(sortType.name());
        }
        return stringBuilder.toString();
    }

    protected String formatProperty(List<Property> properties) {
        if (properties == null || properties.isEmpty()) {
            return StringUtils.EMPTY;
        }
        return properties.stream().map(
            x -> formatStringLiteral(x.getName()) + "=" + formatStringLiteral(x.getValue())
        ).collect(joining(","));
    }

    protected String formatPartitions(List<ColumnDefinition> partitionCol, boolean isNewLine, String elementIndent) {
        StringBuilder stringBuilder = new StringBuilder();
        if (!isNewLine) {
            stringBuilder.append("\n");
        }
        stringBuilder.append("PARTITIONED BY\n(\n");
        String collect = formatColumnList(partitionCol, elementIndent);
        stringBuilder.append(collect);
        stringBuilder.append("\n").append(")\n");
        return stringBuilder.toString();
    }

    protected String formatColumnList(List<ColumnDefinition> list,
                                      String elementIndent) {
        OptionalInt max = list.stream().map(ColumnDefinition::getColName).mapToInt(
            x -> formatExpression(x).length()
        ).max();
        return list.stream()
            .map(element -> elementIndent + formatColumnDefinition(element, max.getAsInt()))
            .collect(joining(",\n"));
    }

    protected String formatColName(Identifier colName, Integer size) {
        String str = formatExpression(colName);
        return StringUtils.rightPad(str, size);
    }

    protected String formatColumnDefinition(ColumnDefinition column, Integer max) {
        BaseDataType dataType = column.getDataType();
        String col = formatColName(column.getColName(), max);
        StringBuilder sb = new StringBuilder().append(col);
        sb.append(formatAliasedName(column.getAliasedName()));
        if (dataType != null) {
            sb.append(" ").append(formatDataType(dataType));
        }

        Optional.ofNullable(column.getCategory()).ifPresent(
            category -> {
                appendCategory(sb, column.getCategory());
            }
        );
        boolean isPrimary = column.getPrimary() != null && column.getPrimary();
        if (isPrimary) {
            sb.append(" PRIMARY KEY");
        }
        boolean isNotNull = column.getNotNull() != null && column.getNotNull();
        if (!isPrimary && isNotNull) {
            sb.append(" NOT NULL");
        } else if (BooleanUtils.isFalse(column.getNotNull())) {
            sb.append(" NULL");
        }
        if (column.getDefaultValue() != null) {
            sb.append(" DEFAULT ").append(formatExpression(column.getDefaultValue()));
        }
        sb.append(formatComment(column.getComment(), isEndNewLine(sb.toString())));
        appendProperties(sb, column.getColumnProperties());
        appendReferenceObjects(sb, column);
        return sb.toString();
    }

    protected String formatDataType(BaseDataType dataType) {
        return formatExpression(convert(dataType));
    }

    protected boolean isEndNewLine(String text) {
        return text.endsWith(NEW_LINE);
    }

    protected String newLine(String text) {
        return NEW_LINE + text + NEW_LINE;
    }

    protected void removeNewLine(StringBuilder text) {
        if (text.toString().endsWith(NEW_LINE)) {
            text.deleteCharAt(text.length() - 1);
        }
    }

    private void appendCategory(StringBuilder sb, ColumnCategory category) {
        if (category == ColumnCategory.ATTRIBUTE) {
            return;
        }
        sb.append(" ").append(category.name());
    }

    private void appendReferenceObjects(StringBuilder sb, ColumnDefinition column) {
        if (column.getRefDimension() != null) {
            sb.append(" REFERENCES ").append(formatName(column.getRefDimension()));
        } else if (column.getRefIndicators() != null && !column.getRefIndicators().isEmpty()) {
            sb.append(" REFERENCES ").append("(").append(
                    column.getRefIndicators().stream()
                        .map(ExpressionFormatter::formatExpression)
                        .collect(joining(",")))
                .append(")");
        }
    }

    /**
     * 转换
     *
     * @param dataType
     * @return
     */
    protected BaseDataType convert(BaseDataType dataType) {
        return dataType;
    }

    protected StringBuilder append(int indent, String value) {
        return builder.append(indentString(indent))
            .append(value);
    }

    @Override
    public Boolean visitQuery(Query node, Integer indent) {
        if (node.getWith() != null) {
            With with = node.getWith();
            append(indent, "WITH");
            if (with.isRecursive()) {
                builder.append(" RECURSIVE");
            }
            builder.append("\n  ");
            Iterator<WithQuery> queries = with.getQueries().iterator();
            while (queries.hasNext()) {
                WithQuery query = queries.next();
                append(indent, formatExpression(query.getName()));
                Optional.ofNullable(query.getColumnNames()).ifPresent(
                    columnNames -> appendAliasColumns(builder, columnNames));
                builder.append(" AS ");
                process(new TableSubQuery(query.getQuery()), indent);
                builder.append('\n');
                if (queries.hasNext()) {
                    builder.append(", ");
                }
            }
        }

        processRelation(node.getQueryBody(), indent);

        if (node.getOrderBy() != null) {
            process(node.getOrderBy(), indent);
        }

        if (node.getOffset() != null) {
            process(node.getOffset(), indent);
        }

        if (node.getLimit() != null) {
            process(node.getLimit(), indent);
        }
        return true;
    }

    private void processRelation(BaseRelation relation, Integer indent) {
        if (relation instanceof Table) {
            builder.append("TABLE ")
                .append(((Table)relation).getName())
                .append('\n');
        } else {
            process(relation, indent);
        }
    }

    @Override
    public Boolean visitQuerySpecification(QuerySpecification node, Integer indent) {
        process(node.getSelect(), indent);

        if (node.getFrom() != null) {
            append(indent, "FROM");
            builder.append('\n');
            append(indent, "  ");
            process(node.getFrom(), indent);
        }

        builder.append('\n');

        if (node.getWhere() != null) {
            append(indent, "WHERE " + formatExpression(node.getWhere()))
                .append('\n');
        }

        if (node.getGroupBy() != null) {
            append(indent, "GROUP BY " + (node.getGroupBy().isDistinct() ? " DISTINCT " : "") + formatGroupBy(
                node.getGroupBy().getGroupingElements())).append('\n');
        }

        if (node.getHaving() != null) {
            append(indent, "HAVING " + formatExpression(node.getHaving()))
                .append('\n');
        }

        if (node.getOrderBy() != null) {
            process(node.getOrderBy(), indent);
        }

        if (node.getOffset() != null) {
            process(node.getOffset(), indent);
        }

        if (node.getLimit() != null) {
            process(node.getLimit(), indent);
        }
        return true;
    }

    /**
     * 可以给到子类进行重写格式化的方法处理
     *
     * @param baseExpression
     * @return
     */
    protected String formatExpression(BaseExpression baseExpression) {
        return ExpressionFormatter.formatExpression(baseExpression);
    }

    @Override
    public Boolean visitAliasedRelation(AliasedRelation node, Integer indent) {
        processRelationSuffix(node.getRelation(), indent + 1);
        builder.append(' ')
            .append(formatExpression(node.getAlias()));
        appendAliasColumns(builder, node.getColumnNames());

        return true;
    }

    private void processRelationSuffix(BaseRelation relation, Integer indent) {
        if ((relation instanceof AliasedRelation) || (relation instanceof SampledRelation)) {
            builder.append("( ");
            process(relation, indent + 1);
            append(indent, ")");
        } else {
            process(relation, indent);
        }
    }

    @Override
    public Boolean visitTableSubQuery(TableSubQuery node, Integer indent) {
        builder.append('(')
            .append('\n');

        process(node.getQuery(), indent + 1);

        append(indent, ") ");

        return true;
    }

    @Override
    public Boolean visitExpression(BaseExpression node, Integer indent) {
        checkArgument(indent == 0, "visitExpression should only be called at root");
        builder.append(formatExpression(node));
        return true;
    }

    @Override
    public Boolean visitUnion(Union node, Integer indent) {
        Iterator<BaseRelation> relations = node.getRelations().iterator();

        while (relations.hasNext()) {
            processRelation(relations.next(), indent);

            if (relations.hasNext()) {
                builder.append("UNION ");
                if (!node.isDistinct()) {
                    builder.append("ALL ");
                }
            }
        }

        return true;
    }

    @Override
    public Boolean visitExcept(Except node, Integer indent) {
        processRelation(node.getLeft(), indent);

        builder.append("EXCEPT ");
        if (!node.isDistinct()) {
            builder.append("ALL ");
        }

        processRelation(node.getRight(), indent);

        return true;
    }

    @Override
    public Boolean visitIntersect(Intersect node, Integer indent) {
        Iterator<BaseRelation> relations = node.getRelations().iterator();

        while (relations.hasNext()) {
            processRelation(relations.next(), indent);

            if (relations.hasNext()) {
                builder.append("INTERSECT ");
                if (!node.isDistinct()) {
                    builder.append("ALL ");
                }
            }
        }

        return true;
    }

    @Override
    public Boolean visitSelect(Select node, Integer indent) {
        append(indent, "SELECT");
        if (node.isDistinct()) {
            builder.append(" DISTINCT");
        }

        if (node.getSelectItems().size() > 1) {
            boolean first = true;
            for (SelectItem item : node.getSelectItems()) {
                builder.append("\n")
                    .append(indentString(indent))
                    .append(first ? "  " : ", ");

                process(item, indent);
                first = false;
            }
        } else {
            builder.append(' ');
            process(getOnlyElement(node.getSelectItems()), indent);
        }

        builder.append('\n');

        return true;
    }

    @Override
    public Boolean visitSingleColumn(SingleColumn node, Integer indent) {
        builder.append(formatExpression(node.getExpression()));
        if (node.getAlias() != null) {
            String as = node.isExistAs() ? " AS " : " ";
            builder.append(as)
                .append(formatExpression(node.getAlias()));
        }
        return true;
    }

    @Override
    public Boolean visitAllColumns(AllColumns node, Integer context) {
        Optional.ofNullable(node.getTarget()).ifPresent(value -> builder
            .append(formatExpression(value))
            .append("."));
        builder.append("*");

        if (!node.getAliases().isEmpty()) {
            String as = node.isExistAs() ? " AS " : " ";
            builder.append(as);
            builder.append("(")
                .append(Joiner.on(", ").join(node.getAliases().stream()
                    .map(this::formatExpression)
                    .collect(toList())))
                .append(")");
        }

        return true;
    }

    @Override
    public Boolean visitTable(Table node, Integer indent) {
        String code = formatName(node.getName());
        builder.append(code);
        return true;
    }

    @Override
    public Boolean visitComment(Comment comment, Integer context) {
        builder.append(formatComment(comment, isEndNewLine(builder.toString())));
        return true;
    }

    @Override
    public Boolean visitJoin(Join node, Integer indent) {
        JoinCriteria criteria = node.getCriteria();
        JoinToken joinToken = node.getJoinToken();
        String type = joinToken.getCode();

        process(node.getLeft(), indent);

        builder.append('\n');
        if (joinToken == IMPLICIT) {
            append(indent, ", ");
        } else {
            append(indent, type).append(" JOIN ");
        }

        process(node.getRight(), indent);

        if (joinToken != CROSS && joinToken != IMPLICIT) {
            if (criteria instanceof JoinUsing) {
                JoinUsing using = (JoinUsing)criteria;
                builder.append(" USING (")
                    .append(Joiner.on(", ").join(using.getColumns()))
                    .append(")");
            } else if (criteria instanceof JoinOn) {
                JoinOn on = (JoinOn)criteria;
                builder.append(" ON ")
                    .append(formatExpression(on.getExpression()));
            }
        }

        return true;
    }

    @Override
    public Boolean visitOrderBy(OrderBy node, Integer indent) {
        append(indent, formatOrderBy(node))
            .append('\n');
        return true;
    }

    private String formatOrderBy(OrderBy node) {
        return new ExpressionVisitor().formatOrderBy(node);
    }

    @Override
    public Boolean visitLimit(Limit node, Integer indent) {
        append(indent, "LIMIT ")
            .append(formatExpression(node.getRowCount()))
            .append('\n');
        return true;
    }

    @Override
    public Boolean visitShowObjects(ShowObjects showObjects, Integer context) {
        builder.append("SHOW ");
        if (showObjects.getFull()) {
            builder.append("FULL ");
        }
        builder.append(showObjects.getShowType().getCode());
        List<QualifiedName> tableName = showObjects.getTableName();
        if (tableName != null && !tableName.isEmpty()) {
            String formatName = formatNames(tableName);
            builder.append(" FROM ").append(formatName);
        } else {
            if (showObjects.getBaseUnit() != null) {
                builder.append(" FROM ").append(formatExpression(showObjects.getBaseUnit()));
            }
        }
        if (showObjects.getConditionElement() != null) {
            process(showObjects.getConditionElement());
        }
        return true;
    }

    private String formatNames(List<QualifiedName> tableName) {
        return tableName.stream()
            .map(e -> formatName(e))
            .collect(joining(","));
    }

    @Override
    public Boolean visitLikeCondition(LikeCondition likeCondition, Integer context) {
        builder.append(" LIKE ").append(formatStringLiteral(likeCondition.getPattern()));
        return true;
    }

    @Override
    public Boolean visitWhereCondition(WhereCondition whereCondition, Integer context) {
        builder.append(" WHERE ").append(formatExpression(whereCondition.getBaseExpression()));
        return true;
    }

    @Override
    public Boolean visitDescribe(Describe describe, Integer context) {
        builder.append("DESC ").append(describe.getDescType().getCode().toUpperCase()).append(" ");
        if (describe.getBaseUnit() != null) {
            builder.append(describe.getBaseUnit()).append(".");
        }
        builder.append(describe.getIdentifier());
        return true;
    }

    @Override
    public Boolean visitUnSetTableProperties(UnSetTableProperties unSetTableProperties, Integer context) {
        builder.append("ALTER TABLE ").append(getCode(unSetTableProperties.getQualifiedName()));
        builder.append(" UNSET PROPERTIES").append('(');
        builder.append(
            unSetTableProperties.getPropertyKeys().stream().map(x -> formatStringLiteral(x)).collect(joining(",")));
        builder.append(')');
        return true;
    }

    @Override
    public Boolean visitSetTableProperties(SetTableProperties setTableProperties, Integer context) {
        builder.append("ALTER TABLE ").append(getCode(setTableProperties.getQualifiedName()));
        builder.append(" SET PROPERTIES").append('(');
        builder.append(formatProperty(setTableProperties.getProperties()));
        builder.append(')');
        return true;
    }

    @Override
    public Boolean visitDropCol(DropCol dropCol, Integer context) {
        builder.append("ALTER TABLE ").append(getCode(dropCol.getQualifiedName()));
        builder.append(" DROP COLUMN ").append(formatExpression(dropCol.getColumnName()));
        return true;
    }

    @Override
    public Boolean visitAddCols(AddCols addCols, Integer context) {
        builder.append("ALTER TABLE ").append(getCode(addCols.getQualifiedName()));
        builder.append(" ADD COLUMNS\n").append('(').append("\n");
        String columnList = formatColumnList(addCols.getColumnDefineList(), indentString(context + 1));
        builder.append(columnList);
        builder.append("\n");
        builder.append(')');
        return true;
    }

    @Override
    public Boolean visitSetTableComment(SetTableComment setTableComment, Integer context) {
        builder.append("ALTER TABLE ").append(getCode(setTableComment.getQualifiedName()));
        builder.append(" SET COMMENT ").append(formatStringLiteral(setTableComment.getComment().getComment()));
        return true;
    }

    @Override
    public Boolean visitChangeCol(ChangeCol renameCol, Integer context) {
        builder.append("ALTER TABLE ").append(getCode(renameCol.getQualifiedName()));
        builder.append(" CHANGE COLUMN ").append(this.formatExpression(renameCol.getOldColName()));
        builder.append(" ").append(formatColumnDefinition(renameCol.getColumnDefinition(), 0));
        return true;
    }

    @Override
    public Boolean visitAddPartitionCol(AddPartitionCol addPartitionCol, Integer context) {
        builder.append("ALTER TABLE ").append(getCode(addPartitionCol.getQualifiedName()));
        builder.append(" ADD PARTITION COLUMN ").append(
            formatColumnDefinition(addPartitionCol.getColumnDefinition(), 0));
        return true;
    }

    @Override
    public Boolean visitDropPartitionCol(DropPartitionCol dropPartitionCol, Integer context) {
        builder.append("ALTER TABLE ").append(getCode(dropPartitionCol.getQualifiedName()));
        builder.append(" DROP PARTITION COLUMN ").append(dropPartitionCol.getColumnName());
        return true;
    }

    @Override
    public Boolean visitDropConstraint(DropConstraint dropConstraint, Integer context) {
        builder.append("ALTER TABLE ").append(getCode(dropConstraint.getQualifiedName()));
        builder.append(" DROP CONSTRAINT ").append(dropConstraint.getConstraintName());
        return true;
    }

    @Override
    public Boolean visitAddConstraint(AddConstraint addConstraint, Integer context) {
        builder.append("ALTER TABLE ").append(getCode(addConstraint.getQualifiedName()));
        builder.append(" ADD ");
        process(addConstraint.getConstraintStatement(), context);
        return true;
    }

    @Override
    public Boolean visitBaseDrop(BaseDrop baseDrop, Integer context) {
        builder.append("DROP ");
        builder.append(baseDrop.getStatementType().getCode().toUpperCase()).append(" ");
        if (baseDrop.isExists()) {
            builder.append("IF EXISTS ");
        }
        builder.append(getCode(baseDrop.getQualifiedName()));
        return true;
    }

    @Override
    public Boolean visitBaseSetComment(BaseSetComment baseSetComment, Integer context) {
        builder.append("ALTER ").append(baseSetComment.getStatementType().getCode().toUpperCase()).append(" ").
            append(getCode(baseSetComment.getQualifiedName())).append(
                " SET").append(formatComment(baseSetComment.getComment(), false));
        return true;
    }

    @Override
    public Boolean visitCreateAdjunct(CreateAdjunct createAdjunct, Integer context) {
        super.visitCreateAdjunct(createAdjunct, context);
        if (createAdjunct.getExpression() != null) {
            builder.append(" AS ");
            String expression = formatExpression(createAdjunct.getExpression());
            builder.append(expression);
        }
        return true;
    }

    @Override
    public Boolean visitBaseCreate(BaseCreate baseCreate, Integer context) {
        appendCreateOrReplace(baseCreate);
        builder.append(baseCreate.getStatementType().getCode().toUpperCase())
            .append(" ").append(getCode(baseCreate.getQualifiedName()));
        if (baseCreate.getAliasedNameValue() != null) {
            builder.append(formatAliasedName(baseCreate.getAliasedName()));
        }
        if (baseCreate.getCommentValue() != null) {
            builder.append(formatComment(baseCreate.getComment(), false));
        }
        builder.append(formatWith(baseCreate.getProperties(), isEndNewLine(builder.toString())));
        return true;
    }

    @Override
    public Boolean visitInsert(Insert insert, Integer context) {
        Boolean overwrite = insert.getOverwrite();
        if (BooleanUtils.isNotTrue(overwrite)) {
            builder.append("INSERT INTO ").append(getCode(insert.getQualifiedName()));
        } else {
            builder.append("INSERT OVERWRITE ").append(getCode(insert.getQualifiedName()));
        }
        appendPartition(builder, insert.getPartitionSpecList(), ",");
        //如果是overwrite，那么不生成列信息
        if (BooleanUtils.isNotTrue(overwrite)) {
            builder.append(" (").append(
                insert.getColumns().stream().map(ExpressionFormatter::formatExpression).collect(joining(","))
            ).append(")");
            builder.append(" \n ");
        }
        process(insert.getQuery(), context);
        return true;
    }

    @Override
    public Boolean visitRenameCol(RenameCol renameCol, Integer context) {
        builder.append("ALTER TABLE ").append(getCode(renameCol.getQualifiedName()));
        builder.append(" CHANGE COLUMN ").append(renameCol.getOldColName());
        builder.append(" RENAME TO ").append(renameCol.getNewColName());
        return true;
    }

    @Override
    public Boolean visitSetColComment(SetColComment setColComment, Integer context) {
        builder.append("ALTER TABLE ").append(getCode(setColComment.getQualifiedName()));
        builder.append(" CHANGE COLUMN ").append(setColComment.getChangeColumn());
        builder.append(formatComment(setColComment.getComment(), false));
        return true;
    }

    @Override
    public Boolean visitBaseUnSetProperties(BaseUnSetProperties baseUnSetProperties, Integer context) {
        builder.append("ALTER ").append(baseUnSetProperties.getStatementType().getCode().toUpperCase());
        builder.append(" ").append(getCode(baseUnSetProperties.getQualifiedName()));
        builder.append(" UNSET PROPERTIES (").append(
            baseUnSetProperties.getPropertyKeys().stream().map(x -> {
                    return formatStringLiteral(x);
                })
                .collect(joining(",")));
        builder.append(")");
        return true;
    }

    @Override
    public Boolean visitBaseSetProperties(BaseSetProperties baseSetProperties, Integer context) {
        builder.append("ALTER ").append(baseSetProperties.getStatementType().getCode().toUpperCase());
        builder.append(" ").append(getCode(baseSetProperties.getQualifiedName()));
        builder.append(" SET PROPERTIES (").append(formatProperty(baseSetProperties.getProperties()));
        builder.append(")");
        return true;
    }

    @Override
    public Boolean visitBaseRename(BaseRename baseRename, Integer context) {
        builder.append("ALTER ").append(baseRename.getStatementType().getCode().toUpperCase());
        builder.append(" ").append(getCode(baseRename.getQualifiedName()));
        builder.append(" RENAME TO ").append(formatName(baseRename.getTarget()));
        return true;
    }

    @Override
    public Boolean visitBaseSetAliasedName(BaseSetAliasedName baseSetAliasedName, Integer context) {
        builder.append("ALTER ").append(baseSetAliasedName.getStatementType().getCode().toUpperCase());
        builder.append(" ").append(getCode(baseSetAliasedName.getQualifiedName()));
        builder.append(" SET ALIAS ").append(formatStringLiteral(baseSetAliasedName.getAliasedName().getName()));
        return true;
    }

    /**
     * ALTER INDICATOR idc1 REFERENCES table1 SET PROPERTIES('key1'='value1') AS sum(a*c)
     *
     * @param setIndicatorProperties addConstraint
     * @param context                context
     * @return
     */
    @Override
    public Boolean visitSetIndicatorProperties(SetIndicatorProperties setIndicatorProperties, Integer context) {
        builder.append("ALTER INDICATOR ").append(getCode(setIndicatorProperties.getQualifiedName()));
        if (setIndicatorProperties.getPrimaryTypeDataType() != null) {
            builder.append(" ").append(formatExpression(setIndicatorProperties.getPrimaryTypeDataType()));
        }
        if (setIndicatorProperties.getReferences() != null) {
            builder.append(" ").append(formatName(setIndicatorProperties.getReferences()));
        }
        builder.append(" SET PROPERTIES (").append(formatProperty(setIndicatorProperties.getProperties())).append(")");
        if (setIndicatorProperties.getExpression() != null) {
            builder.append(" AS ").append(formatExpression(setIndicatorProperties.getExpression()));
        }
        return true;
    }

    //add the createDict

    @Override
    public Boolean visitCreateDict(CreateDict createDict, Integer context) {
        appendCreateOrReplace(createDict);
        builder.append("DICT ");
        builder.append(getCode(createDict.getQualifiedName()));
        builder.append(formatAliasedName(createDict.getAliasedName()));
        if (createDict.getBaseDataType() != null) {
            builder.append(" ").append(createDict.getBaseDataType());
        }
        if (createDict.notNull()) {
            builder.append(" NOT NULL");
        }
        if (createDict.getDefaultValue() != null) {
            builder.append(" DEFAULT ").append(createDict.getDefaultValue());
        }
        builder.append(formatComment(createDict.getComment(), false));
        builder.append(formatWith(createDict.getProperties(), isEndNewLine(builder.toString())));
        return true;
    }

    @Override
    public Boolean visitCreateGroup(CreateGroup createGroup, Integer context) {
        appendCreateOrReplace(createGroup);
        builder.append("GROUP ");
        builder.append(createGroup.getGroupType().name()).append(" ");
        builder.append(getCode(createGroup.getQualifiedName()));
        builder.append(formatAliasedName(createGroup.getAliasedName()));
        builder.append(formatComment(createGroup.getComment(), false));
        builder.append(formatWith(createGroup.getProperties(), isEndNewLine(builder.toString())));
        return true;
    }

    @Override
    public Boolean visitCreateRules(CreateRules createRules, Integer context) {
        builder.append("CREATE ");
        if (createRules.getRulesLevel() != null) {
            builder.append(
                createRules.getRulesLevel().name());
            builder.append(" ");
        }
        builder.append("RULES ");
        builder.append(getCode(createRules.getQualifiedName()));
        builder.append(" REFERENCES ").append(formatName(createRules.getTableName()));
        appendPartition(builder, createRules.getPartitionSpecList(), ",");
        List<RuleDefinition> ruleDefinitions = createRules.getRuleDefinitions();
        if (ruleDefinitions == null) {
            return null;
        }
        builder.append("\n(\n");
        Iterator<RuleDefinition> iterator = ruleDefinitions.iterator();
        while (iterator.hasNext()) {
            process(iterator.next(), context + 1);
            if (iterator.hasNext()) {
                builder.append(",\n");
            }
        }
        builder.append("\n)");
        builder.append(formatComment(createRules.getComment(), isEndNewLine(builder.toString())));
        builder.append(formatWith(createRules.getProperties(), isEndNewLine(builder.toString())));
        return true;
    }

    @Override
    public Boolean visitCreateDqcRule(CreateDqcRule createRules, Integer context) {
        appendCreateOrReplace(createRules);
        builder.append("DQC_RULE ");
        builder.append(formatName(createRules.getQualifiedName()));
        builder.append(" ON TABLE ").append(formatName(createRules.getTableName()));
        appendPartition(builder, createRules.getPartitionSpecList(), ",");
        List<BaseCheckElement> ruleDefinitions = createRules.getBaseCheckElements();
        if (ruleDefinitions == null) {
            return null;
        }
        builder.append("\n(\n");
        Iterator<BaseCheckElement> iterator = ruleDefinitions.iterator();
        while (iterator.hasNext()) {
            process(iterator.next(), context + 1);
            if (iterator.hasNext()) {
                builder.append(",\n");
            }
        }
        builder.append("\n)");
        builder.append(formatComment(createRules.getComment(), isEndNewLine(builder.toString())));
        builder.append(formatWith(createRules.getProperties(), isEndNewLine(builder.toString())));
        return true;
    }

    protected void appendPartition(StringBuilder builder,
                                   List<PartitionSpec> partitionSpecList, String split) {
        if (partitionSpecList == null || partitionSpecList.isEmpty()) {
            return;
        }
        builder.append(" PARTITION ").append("(");
        builder.append(partitionSpecList.stream().map(x -> {
            return formatPartitionSpec(x);
        }).collect(joining(split)));
        builder.append(")");
    }

    @Override
    public Boolean visitBaseCheckElement(BaseCheckElement ruleDefinition, Integer context) {
        String ident = indentString(context);
        builder.append(ident);
        builder.append("CONSTRAINT ");
        builder.append(formatExpression(ruleDefinition.getCheckName()));
        BaseExpression boolExpression = ruleDefinition.getBoolExpression();
        if (boolExpression != null) {
            builder.append(" CHECK");
            boolean notParenthesized = !boolExpression.isParenthesized();
            if (notParenthesized) {
                builder.append("(");
            }
            builder.append(formatExpression(boolExpression));
            if (notParenthesized) {
                builder.append(")");
            }
        }
        Boolean enforced = ruleDefinition.getEnforced();
        if (BooleanUtils.isTrue(enforced)) {
            builder.append(" ENFORCED");
        } else {
            builder.append(" NOT ENFORCED");
        }
        if (!ruleDefinition.isEnable()) {
            builder.append(" ").append("DISABLE");
        }
        return true;
    }

    @Override
    public Boolean visitRuleDefinition(RuleDefinition ruleDefinition, Integer context) {
        String ident = indentString(context);
        builder.append(ident);
        if (ruleDefinition.getRuleGrade() != null) {
            builder.append(ruleDefinition.getRuleGrade());
            builder.append(" ");
        }
        builder.append(formatExpression(ruleDefinition.getRuleName()));
        builder.append(" ");
        AliasedName aliasedName = ruleDefinition.getAliasedName();
        if (aliasedName != null) {
            builder.append(formatAliasedName(aliasedName));
            builder.append(" ");
        }
        if (ruleDefinition.getRuleStrategy() != null) {
            process(ruleDefinition.getRuleStrategy(), context);
        }
        Comment comment = ruleDefinition.getComment();
        if (comment != null) {
            builder.append(" ");
            builder.append(formatComment(comment, false));
        }
        if (!ruleDefinition.isEnable()) {
            builder.append(" ").append("DISABLE");
        }
        return true;
    }

    @Override
    public Boolean visitFixedStrategy(FixedStrategy fixedStrategy, Integer context) {
        builder.append(ExpressionFormatter.formatExpression(fixedStrategy.toExpression()));
        return true;
    }

    @Override
    public Boolean visitVolStrategy(VolStrategy volStrategy, Integer context) {
        builder.append(ExpressionFormatter.formatExpression(volStrategy.toExpression()));
        return true;
    }

    @Override
    public Boolean visitDynamicStrategy(DynamicStrategy dynamicStrategy, Integer context) {
        builder.append(ExpressionFormatter.formatExpression(dynamicStrategy.toExpression()));
        return true;
    }

    private String formatPartitionSpec(PartitionSpec x) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(formatExpression(x.getPartitionColName()));
        stringBuilder.append("=");
        if (x.getBaseLiteral() == null) {
            stringBuilder.append(formatExpression(new StringLiteral("[a-zA-Z0-9_-]*")));
        } else {
            stringBuilder.append(formatExpression(x.getBaseLiteral()));
        }
        return stringBuilder.toString();
    }

    @Override
    public Boolean visitDropRule(DropRule dropRule, Integer context) {
        builder.append("ALTER");
        if (dropRule.getRulesLevel() != null) {
            builder.append(" ");
            builder.append(dropRule.getRulesLevel().name());
        }
        builder.append(" RULES ");
        if (dropRule.getQualifiedName() != null) {
            builder.append(formatName(dropRule.getQualifiedName()));
        } else {
            builder.append("REFERENCES ").append(formatName(dropRule.getTableName()));
            builder.append(formatePartitionSpec(dropRule.getPartitionSpecList()));
        }
        builder.append(" DROP RULE ").append(formatExpression(dropRule.getRuleOrColumn()));
        return false;
    }

    @Override
    public Boolean visitDropDqcRule(DropDqcRule dropRule, Integer context) {
        builder.append("ALTER DQC_RULE ");
        if (dropRule.getQualifiedName() != null && !RuleUtil.isSysGenerate(dropRule.getQualifiedName())) {
            builder.append(getCode(dropRule.getQualifiedName()));
        } else {
            builder.append("ON TABLE ").append(formatName(dropRule.getTableName()));
            builder.append(formatePartitionSpec(dropRule.getPartitionSpecList()));
        }
        builder.append(" DROP ").append(formatExpression(dropRule.getRuleOrColumn()));
        return false;
    }

    @Override
    public Boolean visitAddRules(AddRules addRules, Integer context) {
        builder.append("ALTER RULES ");
        if (addRules.getQualifiedName() != null) {
            builder.append(getCode(addRules.getQualifiedName()));
        } else {
            builder.append("REFERENCES ").append(formatName(addRules.getTableName()));
            builder.append(formatePartitionSpec(addRules.getPartitionSpecList()));
        }
        builder.append("\n").append("ADD RULE (\n");
        Iterator<RuleDefinition> iterator = addRules.getRuleDefinitions().iterator();
        while (iterator.hasNext()) {
            process(iterator.next(), context + 1);
            if (iterator.hasNext()) {
                builder.append(",\n");
            }
        }
        builder.append("\n)");
        return false;
    }

    @Override
    public Boolean visitAddDqcRule(AddDqcRule addRules, Integer context) {
        builder.append("ALTER DQC_RULE ");
        if (addRules.getQualifiedName() != null && !RuleUtil.isSysGenerate(addRules.getQualifiedName())) {
            builder.append(getCode(addRules.getQualifiedName()));
        } else {
            builder.append("ON TABLE ").append(formatName(addRules.getTableName()));
            builder.append(formatePartitionSpec(addRules.getPartitionSpecList()));
        }
        builder.append("\n").append("ADD (\n");
        Iterator<BaseCheckElement> iterator = addRules.getBaseCheckElements().iterator();
        while (iterator.hasNext()) {
            process(iterator.next(), context + 1);
            if (iterator.hasNext()) {
                builder.append(",\n");
            }
        }
        builder.append("\n)");
        return false;
    }

    private String formatePartitionSpec(List<PartitionSpec> partitionSpecList) {
        if (partitionSpecList == null || partitionSpecList.isEmpty()) {
            return StringUtils.EMPTY;
        }
        StringBuilder stringBuilder = new StringBuilder();
        appendPartition(stringBuilder, partitionSpecList, ",");
        return stringBuilder.toString();
    }

    @Override
    public Boolean visitChangeRules(ChangeRules changeRules, Integer context) {
        builder.append("ALTER RULES ");
        if (changeRules.getQualifiedName() != null) {
            builder.append(getCode(changeRules.getQualifiedName()));
        } else {
            builder.append("REFERENCES ").append(formatName(changeRules.getTableName()));
            builder.append(formatePartitionSpec(changeRules.getPartitionSpecList()));
        }
        builder.append("\n").append("CHANGE RULE (\n");
        Iterator<ChangeRuleElement> iterator = changeRules.getRuleDefinitions().iterator();
        while (iterator.hasNext()) {
            process(iterator.next(), context + 1);
            if (iterator.hasNext()) {
                builder.append(",\n");
            }
        }
        builder.append("\n)");
        return false;
    }

    @Override
    public Boolean visitChangeRuleElement(ChangeRuleElement changeRuleElement, Integer context) {
        builder.append(formatExpression(changeRuleElement.getOldRuleName()));
        process(changeRuleElement.getRuleDefinition(), context);
        return false;
    }

    @Override
    public Boolean visitChangeDqcRule(ChangeDqcRule changeRules, Integer context) {
        builder.append("ALTER DQC_RULE ");
        if (changeRules.getQualifiedName() != null &&
            !RuleUtil.isSysGenerate(changeRules.getQualifiedName())) {
            builder.append(getCode(changeRules.getQualifiedName()));
        } else {
            builder.append("ON TABLE ").append(formatName(changeRules.getTableName()));
            builder.append(formatePartitionSpec(changeRules.getPartitionSpecList()));
        }
        builder.append("\n").append("CHANGE (\n");
        Iterator<ChangeDqcRuleElement> iterator = changeRules.getChangeDqcRuleElement().iterator();
        while (iterator.hasNext()) {
            process(iterator.next(), context + 1);
            if (iterator.hasNext()) {
                builder.append(",\n");
            }
        }
        builder.append("\n)");
        return false;
    }

    @Override
    public Boolean visitChangeDqcRuleElement(ChangeDqcRuleElement changeDqcRuleElement, Integer context) {
        builder.append(indentString(context) + formatExpression(changeDqcRuleElement.getOldRuleName()));
        process(changeDqcRuleElement.getBaseCheckElement(), context);
        return false;
    }

    @Override
    public Boolean visitCloneTable(CloneTable cloneTable, Integer context) {
        builder.append("CREATE ");
        builder.append(cloneTable.getTableDetailType().getParent().getCode().toUpperCase(Locale.ROOT));
        builder.append(" TABLE ");
        if (cloneTable.isNotExists()) {
            builder.append("IF NOT EXISTS ");
        }
        builder.append(getCode(cloneTable.getQualifiedName()));
        builder.append(" LIKE ");
        builder.append(formatName(cloneTable.getSourceTable()));
        return true;
    }

    @Override
    public Boolean visitListNode(ListNode listNode, Integer context) {
        List<? extends Node> children = listNode.getChildren();
        children.stream().forEach(n -> {
            process(n, context);
            builder.append(";\n");
        });
        //删除最后一个换行
        builder.deleteCharAt(builder.toString().length() - 1);
        return true;
    }

    @Override
    public Boolean visitImportEntityStatement(ImportObject importEntityStatement, Integer context) {
        builder.append("IMPORT ");
        builder.append(getCode(importEntityStatement.getQualifiedName()));
        if (importEntityStatement.getAlias() != null) {
            builder.append(" AS ");
            builder.append(formatExpression(importEntityStatement.getAlias()));
        }
        return false;
    }

    @Override
    public Boolean visitRefEntityStatement(RefRelation refEntityStatement, Integer context) {
        builder.append("REF ");
        builder.append(formatRefEntity(refEntityStatement.getLeft()));
        builder.append(" ");
        builder.append(refEntityStatement.getRefDirection().getCode());
        builder.append(" ");
        builder.append(formatRefEntity(refEntityStatement.getRight()));
        QualifiedName qualifiedName = refEntityStatement.getQualifiedName();
        String suffix = qualifiedName.getSuffix();
        if (!IdentifierUtil.isSysIdentifier(new Identifier(suffix))) {
            builder.append(" : ").append(suffix);
        }
        return false;
    }

    @Override
    public Boolean visitCreateDimension(CreateDimension createDimension, Integer context) {
        appendCreateOrReplace(createDimension);
        builder.append("DIMENSION ");
        if (createDimension.isNotExists()) {
            builder.append("IF NOT EXISTS ");
        }
        builder.append(getCode(createDimension.getQualifiedName()));
        builder.append(formatAliasedName(createDimension.getAliasedName()));
        if (!createDimension.isAttributeEmpty()) {
            String elementIndent = indentString(context + 1);
            builder.append(newLine("("));
            builder.append(formatAttributes(createDimension.getDimensionAttributes(), elementIndent));
            builder.append(newLine(")"));
        }
        builder.append(formatComment(createDimension.getComment(), isEndNewLine(builder.toString())));
        builder.append(formatWith(createDimension.getProperties(), isEndNewLine(builder.toString())));
        removeNewLine(builder);
        //默认false，物理引擎不支持
        return false;
    }

    @Override
    public Boolean visitSetColumnOrder(SetColumnOrder setColumnOrder, Integer context) {
        builder.append("ALTER TABLE ");
        builder.append(getCode(setColumnOrder.getQualifiedName()));
        builder.append(" CHANGE COLUMN ").append(this.formatExpression(setColumnOrder.getOldColName()));
        ColumnDefinition columnDefinition = ColumnDefinition.builder()
            .colName(setColumnOrder.getNewColName())
            .dataType(setColumnOrder.getDataType())
            .build();
        builder.append(" ");
        builder.append(formatColumnDefinition(columnDefinition, 0));
        if (setColumnOrder.getBeforeColName() != null) {
            builder.append(" AFTER ").append(formatExpression(setColumnOrder.getBeforeColName()));
        } else if (BooleanUtils.isTrue(setColumnOrder.getFirst())) {
            builder.append(" FIRST");
        }
        //默认是false，目前mc只有弹内支持，弹外不支持，所以先设置为false，物理引擎不支持
        return false;
    }

    @Override
    public Boolean visitMoveReferences(MoveReferences moveReferences, Integer context) {
        builder.append("MOVE ").append(moveReferences.getShowType().name());
        builder.append(" REFERENCES ");
        builder.append(getCode(moveReferences.getFrom()));
        builder.append(" TO ");
        builder.append(formatName(moveReferences.getTo()));
        builder.append(formatWith(moveReferences.getProperties(), false));
        return false;
    }

    private String formatAttributes(List<DimensionAttribute> dimensionFields, String elementIndent) {
        OptionalInt max = dimensionFields.stream().map(DimensionAttribute::getAttrName).mapToInt(
            x -> {
                return formatExpression(x).length();
            }
        ).max();
        String columnList = dimensionFields.stream()
            .map(element -> elementIndent + formatDimensionAttribute(element,
                max.getAsInt()))
            .collect(joining(",\n"));
        return columnList;
    }

    private String formatDimensionAttribute(DimensionAttribute element, int max) {
        String col = formatColName(element.getAttrName(), max);
        StringBuilder stringBuilder = new StringBuilder(col);
        stringBuilder.append(formatAliasedName(element.getAliasedName()));
        if (element.getCategory() != AttributeCategory.BUSINESS) {
            stringBuilder.append(" ").append(element.getCategory().name());
        }
        if (BooleanUtils.isTrue(element.getPrimary())) {
            stringBuilder.append(" ").append("PRIMARY KEY");
        }
        stringBuilder.append(formatComment(element.getComment()));
        stringBuilder.append(formatWith(element.getProperties(), isEndNewLine(stringBuilder.toString())));
        return stringBuilder.toString();
    }

    @Override
    public Boolean visitAddDimensionField(AddDimensionAttribute addDimensionAttribute, Integer context) {
        builder.append("ALTER DIMENSION ");
        builder.append(getCode(addDimensionAttribute.getQualifiedName()));
        builder.append(" ADD ATTRIBUTES ");
        builder.append(newLine("("));
        String indent = indentString(context + 1);
        builder.append(formatAttributes(addDimensionAttribute.getDimensionAttributes(), indent));
        builder.append(newLine(")"));
        removeNewLine(builder);
        return true;
    }

    @Override
    public Boolean visitDropDimensionField(DropDimensionAttribute dropDimensionAttribute, Integer context) {
        builder.append("ALTER DIMENSION ");
        builder.append(formatName(dropDimensionAttribute.getQualifiedName()));
        builder.append(" DROP ATTRIBUTE ").append(formatExpression(dropDimensionAttribute.getAttrName()));
        return true;
    }

    @Override
    public Boolean visitChangeDimensionField(ChangeDimensionAttribute changeDimensionAttribute, Integer context) {
        builder.append("ALTER DIMENSION ");
        builder.append(getCode(changeDimensionAttribute.getQualifiedName()));
        builder.append(" CHANGE ATTRIBUTE ");
        builder.append(formatExpression(changeDimensionAttribute.getNeedChangeAttr()));
        builder.append(" ");
        builder.append(formatDimensionAttribute(changeDimensionAttribute.getDimensionAttribute(), 0));
        return true;
    }

    private String formatRefEntity(RefObject refObject) {
        StringBuilder stringBuilder = new StringBuilder();
        if (!refObject.getAttrNameList().isEmpty()) {
            stringBuilder.append(
                refObject.getAttrNameList().stream().map(
                    identifier -> {
                        List<Identifier> list = Lists.newArrayList(refObject.getMainName().getOriginalParts());
                        list.add(identifier);
                        QualifiedName q = QualifiedName.of(list);
                        return formatName(q);
                    }
                ).collect(joining(","))
            );
        } else {
            stringBuilder.append(formatName(refObject.getMainName()));
        }
        if (refObject.getCommentValue() != null) {
            stringBuilder.append(formatComment(refObject.getComment()));
        }
        return stringBuilder.toString();
    }

    private String formatWith(List<Property> properties, boolean isNewLine) {
        if (properties == null || properties.isEmpty()) {
            return "";
        }
        StringBuilder stringBuilder = new StringBuilder();
        if (!isNewLine) {
            stringBuilder.append("\n");
        }
        stringBuilder.append("WITH(");
        stringBuilder.append(formatProperty(properties));
        stringBuilder.append(")");
        return stringBuilder.toString();
    }

    protected String indentString(int indent) {
        return Strings.repeat(INDENT, indent);
    }

    protected String formatGroupBy(List<GroupingElement> groupingElements) {
        ImmutableList.Builder<String> resultStrings = ImmutableList.builder();

        for (GroupingElement groupingElement : groupingElements) {
            String result = "";
            if (groupingElement instanceof SimpleGroupBy) {
                List<BaseExpression> columns = groupingElement.getExpressions();
                if (columns.size() == 1) {
                    result = formatExpression(getOnlyElement(columns));
                } else {
                    result = formatSimpleGroupBy(columns);
                }
            } else if (groupingElement instanceof GroupingSets) {
                result = String.format("GROUPING SETS (%s)", Joiner.on(", ").join(
                    ((GroupingSets)groupingElement).getSets().stream()
                        .map(g -> {
                            return formatGroupingSet(g);
                        })
                        .iterator()));
            } else if (groupingElement instanceof Cube) {
                result = String.format("CUBE %s", formatGroupingSet(groupingElement.getExpressions()));
            } else if (groupingElement instanceof Rollup) {
                result = String.format("ROLLUP %s", formatGroupingSet(groupingElement.getExpressions()));
            }
            resultStrings.add(result);
        }
        return Joiner.on(", ").join(resultStrings.build());
    }

    protected String formatStringLiteral(String s) {
        if (s == null) {
            return null;
        }
        String result = s.replace("'", "''");
        return "'" + result + "'";
    }

    public String formatName(QualifiedName name) {
        return name.getOriginalParts().stream()
            .map(this::formatExpression)
            .collect(joining("."));
    }

    public String formatGroupingSet(List<BaseExpression> groupingSet) {
        return format("(%s)", Joiner.on(", ").join(groupingSet.stream()
            .map(ExpressionFormatter::formatExpression)
            .iterator()));
    }

    private String formatSimpleGroupBy(List<BaseExpression> columns) {
        return columns.stream().map(ExpressionFormatter::formatExpression).collect(joining(","));
    }

    protected void appendAliasColumns(StringBuilder builder, List<Identifier> columns) {
        if ((columns != null) && (!columns.isEmpty())) {
            String formattedColumns = columns.stream()
                .map(this::formatExpression)
                .collect(Collectors.joining(", "));
            builder.append(" (")
                .append(formattedColumns)
                .append(')');
        }
    }
}
