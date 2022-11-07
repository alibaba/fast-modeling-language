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

package com.aliyun.fastmodel.transform.hive.parser;

import java.util.List;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.common.parser.ParserHelper;
import com.aliyun.fastmodel.common.utils.StripUtils;
import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeParameter;
import com.aliyun.fastmodel.core.tree.datatype.Field;
import com.aliyun.fastmodel.core.tree.datatype.GenericDataType;
import com.aliyun.fastmodel.core.tree.datatype.NumericParameter;
import com.aliyun.fastmodel.core.tree.datatype.RowDataType;
import com.aliyun.fastmodel.core.tree.datatype.TypeParameter;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.literal.StringLiteral;
import com.aliyun.fastmodel.core.tree.statement.CompositeStatement;
import com.aliyun.fastmodel.core.tree.statement.element.CreateElement;
import com.aliyun.fastmodel.core.tree.statement.table.CloneTable;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.PartitionedBy;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.BaseConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.DimConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.PrimaryConstraint;
import com.aliyun.fastmodel.core.tree.util.IdentifierUtil;
import com.aliyun.fastmodel.transform.api.context.ReverseContext;
import com.aliyun.fastmodel.transform.hive.parser.HiveParser.ColumnNameColonTypeContext;
import com.aliyun.fastmodel.transform.hive.parser.HiveParser.ColumnNameContext;
import com.aliyun.fastmodel.transform.hive.parser.HiveParser.ColumnNameTypeConstraintContext;
import com.aliyun.fastmodel.transform.hive.parser.HiveParser.ColumnNameTypeOrConstraintListContext;
import com.aliyun.fastmodel.transform.hive.parser.HiveParser.CreateConstraintContext;
import com.aliyun.fastmodel.transform.hive.parser.HiveParser.CreateForeignKeyContext;
import com.aliyun.fastmodel.transform.hive.parser.HiveParser.CreateTableStatementContext;
import com.aliyun.fastmodel.transform.hive.parser.HiveParser.GenericTypeContext;
import com.aliyun.fastmodel.transform.hive.parser.HiveParser.IdentifierContext;
import com.aliyun.fastmodel.transform.hive.parser.HiveParser.KeyValuePropertyContext;
import com.aliyun.fastmodel.transform.hive.parser.HiveParser.PkConstraintContext;
import com.aliyun.fastmodel.transform.hive.parser.HiveParser.StatementsContext;
import com.aliyun.fastmodel.transform.hive.parser.HiveParser.TableCommentContext;
import com.aliyun.fastmodel.transform.hive.parser.HiveParser.TableNameContext;
import com.aliyun.fastmodel.transform.hive.parser.HiveParser.TablePartitionContext;
import com.aliyun.fastmodel.transform.hive.parser.HiveParser.TablePropertiesPrefixedContext;
import com.aliyun.fastmodel.transform.hive.parser.HiveParser.TypeParameterContext;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.antlr.v4.runtime.tree.TerminalNode;

import static com.aliyun.fastmodel.common.parser.ParserHelper.getLocation;
import static com.aliyun.fastmodel.common.parser.ParserHelper.getOrigin;
import static java.util.stream.Collectors.toList;

/**
 * AstBuilder
 *
 * @author panguanjing
 * @date 2021/9/4
 */
public class HiveAstBuilder extends HiveParserBaseVisitor<Node> {

    private final ReverseContext reverseContext;

    public HiveAstBuilder(ReverseContext context) {
        reverseContext = context == null ? ReverseContext.builder().build() : context;
    }

    @Override
    public Node visitStatements(StatementsContext ctx) {
        List<BaseStatement> visit = ParserHelper.visit(this, ctx.statement(), BaseStatement.class);
        if (visit.size() == 1) {
            return visit.get(0);
        }
        return new CompositeStatement(visit);
    }

    @Override
    public Node visitCreateTableStatement(CreateTableStatementContext ctx) {
        TerminalNode terminalNode = ctx.KW_LIKE();
        if (terminalNode != null) {
            QualifiedName visit = (QualifiedName)visit(ctx.tableName(0));
            TablePropertiesPrefixedContext tablePropertiesPrefixedContext = ctx.tablePropertiesPrefixed();
            List<Property> list = ImmutableList.of();
            if (tablePropertiesPrefixedContext != null) {
                list = ParserHelper.visit(this,
                    tablePropertiesPrefixedContext.tableProperties().tablePropertiesList().keyProperty(),
                    Property.class);
            }
            return new CloneTable(
                CreateElement.builder()
                    .qualifiedName(visit)
                    .properties(list)
                    .build(),
                reverseContext.getReverseTableType(),
                (QualifiedName)visit(ctx.tableName(1))
            );
        } else {
            QualifiedName tableName = (QualifiedName)visit(ctx.tableName(0));
            Comment comment = null;
            if (ctx.tableComment() != null) {
                comment = (Comment)visit(ctx.tableComment());
            }
            List<Property> all = Lists.newArrayList(reverseContext.getProperties() == null ? Lists.newArrayList() : reverseContext.getProperties());
            List<Property> properties = ImmutableList.of();
            if (ctx.tablePropertiesPrefixed() != null) {
                properties = ParserHelper.visit(this,
                    ctx.tablePropertiesPrefixed().tableProperties().tablePropertiesList().keyValueProperty(),
                    Property.class);
                all.addAll(properties);
            }
            ColumnNameTypeOrConstraintListContext columnNameTypeOrConstraintListContext
                = ctx.columnNameTypeOrConstraintList();
            List<Node> nodes = Lists.newArrayListWithCapacity(64);
            if (columnNameTypeOrConstraintListContext != null) {
                nodes = ParserHelper.visit(
                    this,
                    columnNameTypeOrConstraintListContext.columnNameTypeOrConstraint(),
                    Node.class
                );
            }
            List<ColumnDefinition> list = nodes.stream().filter(
                node -> {
                    return node instanceof ColumnDefinition;
                }
            ).map(x -> {
                return (ColumnDefinition)x;
            }).collect(Collectors.toList());

            List<BaseConstraint> constraints = nodes.stream().filter(
                node -> {
                    return node instanceof BaseConstraint;
                }
            ).map(x -> {
                return (BaseConstraint)x;
            }).collect(Collectors.toList());

            PartitionedBy partitionBy = ParserHelper.visitIfPresent(this, ctx.tablePartition(), PartitionedBy.class)
                .orElse(null);

            return CreateTable.builder()
                .tableName(tableName)
                .detailType(reverseContext.getReverseTableType())
                .ifNotExist(ctx.ifNotExists() != null)
                .columns(list)
                .constraints(constraints)
                .partition(partitionBy)
                .comment(comment)
                .properties(all)
                .build();
        }
    }

    @Override
    public Node visitTableComment(TableCommentContext ctx) {
        StringLiteral stringLiteral = getStringLiteral(ctx.StringLiteral());
        return new Comment(stringLiteral.getValue());
    }

    @Override
    public Node visitTablePartition(TablePartitionContext ctx) {
        List<ColumnDefinition> list = ParserHelper.visit(this, ctx.columnNameTypeConstraint(), ColumnDefinition.class);
        return new PartitionedBy(list);
    }

    @Override
    public Node visitCreateConstraint(CreateConstraintContext ctx) {
        Identifier identifier = ParserHelper.visitIfPresent(this, ctx.identifier(), Identifier.class)
            .orElse(IdentifierUtil.sysIdentifier());
        PkConstraintContext pkConstraintContext = ctx.pkConstraint();
        List<Identifier> list = ParserHelper.visit(this, pkConstraintContext.pkCols
            .columnNameList().columnName(), Identifier.class);
        return new PrimaryConstraint(identifier, list);
    }

    @Override
    public Node visitCreateForeignKey(CreateForeignKeyContext ctx) {
        Identifier constraint = ParserHelper.visitIfPresent(this, ctx.identifier(), Identifier.class)
            .orElse(IdentifierUtil.sysIdentifier());
        List<Identifier> list = ParserHelper.visit(this, ctx.left
            .columnNameList().columnName(), Identifier.class);

        List<Identifier> right = ParserHelper.visit(this, ctx.right
            .columnNameList().columnName(), Identifier.class);

        QualifiedName tableName = (QualifiedName)visit(ctx.tableName());
        return new DimConstraint(constraint, list, tableName, right);
    }

    @Override
    public Node visitColumnName(ColumnNameContext ctx) {
        return visit(ctx.identifier());
    }

    @Override
    public Node visitColumnNameTypeConstraint(ColumnNameTypeConstraintContext ctx) {
        BaseDataType baseDataType = (BaseDataType)visit(ctx.colType());
        Comment comment = null;
        if (ctx.KW_COMMENT() != null) {
            StringLiteral stringLiteral = getStringLiteral(ctx.StringLiteral());
            comment = new Comment(stringLiteral.getValue());
        }
        BaseConstraint baseConstraint = ParserHelper
            .visitIfPresent(this, ctx.columnConstraint(), BaseConstraint.class)
            .orElse(null);
        Boolean primary = isPrimary(baseConstraint);
        return ColumnDefinition.builder()
            .colName((Identifier)visit(ctx.identifier()))
            .dataType(baseDataType)
            .comment(comment)
            .primary(primary)
            .build();
    }

    @Override
    public Node visitColumnNameColonType(ColumnNameColonTypeContext ctx) {
        Identifier identifier = (Identifier)visit(ctx.identifier());
        BaseDataType baseDataType = (BaseDataType)visit(ctx.colType());
        Comment comment = null;
        if (ctx.StringLiteral() != null) {
            StringLiteral stringLiteral = getStringLiteral(ctx.StringLiteral());
            comment = new Comment(stringLiteral.getValue());
        }
        return new Field(identifier, baseDataType, comment);
    }

    @Override
    public Node visitColType(HiveParser.ColTypeContext ctx) {
        return visit(ctx.type_db_col());
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
    public Node visitTypeParameter(TypeParameterContext ctx) {
        return new NumericParameter(ctx.getText());
    }

    @Override
    public Node visitListType(HiveParser.ListTypeContext ctx) {
        return new GenericDataType(
            getLocation(ctx),
            getOrigin(ctx),
            ctx.KW_ARRAY().getText(),
            ImmutableList.of(new TypeParameter((BaseDataType)visit(ctx.type_db_col()))));
    }

    @Override
    public Node visitMapType(HiveParser.MapTypeContext ctx) {
        return new GenericDataType(
            getLocation(ctx),
            getOrigin(ctx),
            ctx.KW_MAP().getText(),
            ImmutableList.of(
                new TypeParameter((BaseDataType)visit(ctx.key)),
                new TypeParameter((BaseDataType)visit(ctx.value))));
    }

    @Override
    public Node visitStructType(HiveParser.StructTypeContext ctx) {
        List<Field> list = ParserHelper.visit(this, ctx.columnNameColonTypeList().columnNameColonType(), Field.class);
        return new RowDataType(list);
    }




    private Boolean isPrimary(BaseConstraint baseConstraint) {
        return baseConstraint instanceof PrimaryConstraint;
    }

    @Override
    public Node visitTableName(TableNameContext ctx) {
        List<Identifier> list = ParserHelper.visit(this, ctx.identifier(), Identifier.class);
        return QualifiedName.of(list);
    }

    @Override
    public Node visitKeyValueProperty(KeyValuePropertyContext ctx) {
        StringLiteral key = getStringLiteral(ctx.StringLiteral(0));
        StringLiteral value = getStringLiteral(ctx.StringLiteral(1));
        return new Property(
            key.getValue(),
            value
        );
    }

    private StringLiteral getStringLiteral(TerminalNode terminalNode) {
        return new StringLiteral(StripUtils.strip(terminalNode.getText()));
    }

    @Override
    public Node visitIdentifier(IdentifierContext ctx) {
        return ParserHelper.getIdentifier(ctx);
    }
}
