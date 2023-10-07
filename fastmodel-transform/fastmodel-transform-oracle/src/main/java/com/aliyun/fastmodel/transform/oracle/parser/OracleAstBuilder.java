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

package com.aliyun.fastmodel.transform.oracle.parser;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.common.parser.ParserHelper;
import com.aliyun.fastmodel.common.utils.StripUtils;
import com.aliyun.fastmodel.core.exception.ParseException;
import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeParameter;
import com.aliyun.fastmodel.core.tree.datatype.GenericDataType;
import com.aliyun.fastmodel.core.tree.datatype.NumericParameter;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.literal.StringLiteral;
import com.aliyun.fastmodel.core.tree.statement.CompositeStatement;
import com.aliyun.fastmodel.core.tree.statement.constants.ConstraintType;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.SetColComment;
import com.aliyun.fastmodel.core.tree.statement.table.SetTableComment;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.BaseConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.DimConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.NotNullConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.PrimaryConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.UniqueConstraint;
import com.aliyun.fastmodel.core.tree.util.IdentifierUtil;
import com.aliyun.fastmodel.transform.api.context.ReverseContext;
import com.aliyun.fastmodel.transform.oracle.parser.PlSqlParser.Column_definitionContext;
import com.aliyun.fastmodel.transform.oracle.parser.PlSqlParser.Column_nameContext;
import com.aliyun.fastmodel.transform.oracle.parser.PlSqlParser.Comment_on_columnContext;
import com.aliyun.fastmodel.transform.oracle.parser.PlSqlParser.Comment_on_tableContext;
import com.aliyun.fastmodel.transform.oracle.parser.PlSqlParser.Constraint_nameContext;
import com.aliyun.fastmodel.transform.oracle.parser.PlSqlParser.Constraint_stateContext;
import com.aliyun.fastmodel.transform.oracle.parser.PlSqlParser.Create_tableContext;
import com.aliyun.fastmodel.transform.oracle.parser.PlSqlParser.DatatypeContext;
import com.aliyun.fastmodel.transform.oracle.parser.PlSqlParser.Foreign_key_clauseContext;
import com.aliyun.fastmodel.transform.oracle.parser.PlSqlParser.Id_expressionContext;
import com.aliyun.fastmodel.transform.oracle.parser.PlSqlParser.IdentifierContext;
import com.aliyun.fastmodel.transform.oracle.parser.PlSqlParser.Inline_constraintContext;
import com.aliyun.fastmodel.transform.oracle.parser.PlSqlParser.Native_datatype_elementContext;
import com.aliyun.fastmodel.transform.oracle.parser.PlSqlParser.NumericContext;
import com.aliyun.fastmodel.transform.oracle.parser.PlSqlParser.Out_of_line_constraintContext;
import com.aliyun.fastmodel.transform.oracle.parser.PlSqlParser.Quoted_stringContext;
import com.aliyun.fastmodel.transform.oracle.parser.PlSqlParser.Sql_scriptContext;
import com.aliyun.fastmodel.transform.oracle.parser.PlSqlParser.Tableview_nameContext;
import com.aliyun.fastmodel.transform.oracle.parser.PlSqlParser.Type_nameContext;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;

/**
 * OracleAstBuilder
 *
 * @author panguanjing
 * @date 2021/7/24
 */
public class OracleAstBuilder extends PlSqlParserBaseVisitor<Node> {
    private final ReverseContext context;

    public OracleAstBuilder(ReverseContext context) {
        this.context = context == null ? ReverseContext.builder().build() : context;
    }

    @Override
    public Node visitSql_script(Sql_scriptContext ctx) {
        List<BaseStatement> visit = ParserHelper.visit(this, ctx.unit_statement(), BaseStatement.class);
        if (CollectionUtils.isEmpty(visit)) {
            throw new ParseException("invalid statement");
        }
        if (visit.size() > 1) {
            CompositeStatement compositeStatement = new CompositeStatement(visit);
            return compositeStatement;
        } else {
            return visit.get(0);
        }
    }

    @Override
    public Node visitCreate_table(Create_tableContext ctx) {
        Tableview_nameContext tableview_nameContext = ctx.tableview_name();
        QualifiedName qualifiedName = getQualifiedName(tableview_nameContext.identifier(),
            tableview_nameContext.id_expression() != null ?
                ImmutableList.of(tableview_nameContext.id_expression())
                : null);
        List<Node> list = ParserHelper.visit(this, ctx.relational_table().relational_property(), Node.class);
        List<ColumnDefinition> columnDefinitions = list.stream()
            .filter(x -> {
                return x instanceof ColumnDefinition;
            })
            .map(x -> {
                return (ColumnDefinition)x;
            })
            .collect(Collectors.toList());
        List<BaseConstraint> constraints = list.stream().filter(
            x -> {
                return x instanceof BaseConstraint;
            }
        ).map(x -> {
            return (BaseConstraint)x;
        }).collect(Collectors.toList());
        return CreateTable.builder()
            .tableName(qualifiedName)
            .detailType(context.getReverseTableType())
            .columns(columnDefinitions)
            .constraints(constraints)
            .build();
    }

    @Override
    public Node visitComment_on_column(Comment_on_columnContext ctx) {
        Column_nameContext column_nameContext = ctx.column_name();
        QualifiedName identifier = (QualifiedName)visit(column_nameContext);
        StringLiteral comment = (StringLiteral)visit(ctx.quoted_string());
        return new SetColComment(
            QualifiedName.of(identifier.getFirst()),
            getColName(identifier),
            new Comment(comment.getValue()));
    }

    @Override
    public Node visitComment_on_table(Comment_on_tableContext ctx) {
        QualifiedName tableName = getQualifiedName(ctx.tableview_name());
        StringLiteral comment = (StringLiteral)visit(ctx.quoted_string());
        return new SetTableComment(tableName, new Comment(comment.getValue()));
    }

    @Override
    public Node visitQuoted_string(Quoted_stringContext ctx) {
        StringLiteral stringLiteral = new StringLiteral(StripUtils.strip(ctx.getText()));
        return stringLiteral;
    }

    @Override
    public Node visitColumn_definition(Column_definitionContext ctx) {
        Identifier qualifiedName = getColName((QualifiedName)visit(ctx.column_name()));
        BaseDataType baseDataType = null;
        if (ctx.type_name() != null) {
            baseDataType = (BaseDataType)visit(ctx.type_name());
        } else if (ctx.datatype() != null) {
            baseDataType = (BaseDataType)visit(ctx.datatype());
        }
        List<BaseConstraint> baseConstraints = ImmutableList.of();
        if (ctx.inline_constraint() != null) {
            baseConstraints = ParserHelper.visit(this, ctx.inline_constraint(), BaseConstraint.class);
        }
        return ColumnDefinition.builder()
            .colName(qualifiedName)
            .dataType(baseDataType)
            .notNull(notNull(baseConstraints))
            .primary(primary(baseConstraints))
            .build();
    }

    private Boolean notNull(List<BaseConstraint> constraints) {
        Optional<BaseConstraint> first = constraints.stream().filter(x -> {
            return x.getConstraintType() == ConstraintType.NOT_NULL;
        }).findFirst();
        if (!first.isPresent()) {
            return null;
        }
        return first.get().getEnable();
    }

    private Boolean primary(List<BaseConstraint> constraints) {
        Optional<BaseConstraint> first = constraints.stream().filter(x -> {
            return x.getConstraintType() == ConstraintType.PRIMARY_KEY;
        }).findFirst();
        if (!first.isPresent()) {
            return null;
        }
        return first.get().getEnable();
    }

    @Override
    public Node visitDatatype(DatatypeContext ctx) {
        Native_datatype_elementContext native_datatype_elementContext = ctx.native_datatype_element();
        String text = native_datatype_elementContext.getText();
        DataTypeEnums dataTypeEnums = null;
        dataTypeEnums = DataTypeEnums.getDataType(text);
        List<DataTypeParameter> arguments = ImmutableList.of();
        if (ctx.precision_part() != null) {
            arguments = ParserHelper.visit(this, ctx.precision_part().numeric(), DataTypeParameter.class);
        }
        if (dataTypeEnums == DataTypeEnums.CUSTOM) {
            return new GenericDataType(new Identifier(text), arguments);
        } else {
            return new GenericDataType(new Identifier(dataTypeEnums.name()), arguments);
        }
    }

    @Override
    public Node visitNumeric(NumericContext ctx) {
        return new NumericParameter(ctx.getText());
    }

    @Override
    public Node visitType_name(Type_nameContext ctx) {
        List<Identifier> list = ParserHelper.visit(this, ctx.id_expression(), Identifier.class);
        return new GenericDataType(list.get(0));
    }

    @Override
    public Node visitColumn_name(Column_nameContext ctx) {
        QualifiedName qualifiedName = getQualifiedName(ctx.identifier(), ctx.id_expression());
        return qualifiedName;
    }

    @Override
    public Node visitIdentifier(IdentifierContext ctx) {
        return ParserHelper.getIdentifier(ctx);
    }

    @Override
    public Node visitId_expression(Id_expressionContext ctx) {
        return ParserHelper.getIdentifier(ctx);
    }

    @Override
    public Node visitInline_constraint(Inline_constraintContext ctx) {
        Constraint_nameContext constraint_nameContext = ctx.constraint_name();
        Identifier constraintName = null;
        if (constraint_nameContext != null) {
            constraintName = new Identifier(
                getQualifiedName(constraint_nameContext.identifier(),
                    constraint_nameContext.id_expression()).getSuffix());
        } else {
            constraintName = IdentifierUtil.sysIdentifier();
        }
        Constraint_stateContext constraint_stateContext = ctx.constraint_state();
        if (ctx.PRIMARY() != null) {
            return new PrimaryConstraint(constraintName, ImmutableList.of());
        }
        boolean notNull = ctx.NOT() != null && ctx.NULL_() != null;
        if (notNull) {
            return new NotNullConstraint(constraintName);
        }
        return null;
    }

    @Override
    public Node visitOut_of_line_constraint(Out_of_line_constraintContext ctx) {
        boolean isPrimary = ctx.PRIMARY() != null;
        Identifier constraintName = null;
        Constraint_nameContext constraint_nameContext = ctx.constraint_name();
        if (constraint_nameContext != null) {
            constraintName = new Identifier(
                getQualifiedName(constraint_nameContext.identifier(),
                    constraint_nameContext.id_expression()).getSuffix());
        } else {
            constraintName = IdentifierUtil.sysIdentifier();
        }
        if (isPrimary) {
            List<Identifier> list = getIdentifiers(ctx.column_name());
            return new PrimaryConstraint(constraintName, list);
        }
        Foreign_key_clauseContext foreign_key_clauseContext = ctx.foreign_key_clause();
        boolean forigenKey = foreign_key_clauseContext != null;
        if (forigenKey) {
            List<Column_nameContext> contexts = foreign_key_clauseContext.paren_column_list().column_list()
                .column_name();
            List<Identifier> list = getIdentifiers(contexts);

            List<Column_nameContext> column_nameContexts = foreign_key_clauseContext.references_clause()
                .paren_column_list()
                .column_list().column_name();

            List<Identifier> reference = getIdentifiers(column_nameContexts);
            DimConstraint dimConstraint = new DimConstraint(
                constraintName,
                list,
                getQualifiedName(foreign_key_clauseContext.references_clause().tableview_name()),
                reference
            );
            return dimConstraint;
        }
        boolean isUnique = ctx.UNIQUE() != null;
        if (isUnique) {
            List<Identifier> colName = getIdentifiers(ctx.column_name());
            return new UniqueConstraint(constraintName, colName, true);
        }
        return null;
    }

    private List<Identifier> getIdentifiers(List<Column_nameContext> ctx) {
        List<QualifiedName> colNames = ParserHelper.visit(this, ctx, QualifiedName.class);
        List<Identifier> list = colNames.stream().map(x -> {
            return getColName(x);
        }).collect(Collectors.toList());
        return list;
    }

    private Identifier getColName(QualifiedName qualifiedName) {
        return new Identifier(qualifiedName.getSuffix());
    }

    private QualifiedName getQualifiedName(Tableview_nameContext tableview_nameContext) {
        return getQualifiedName(tableview_nameContext.identifier(), tableview_nameContext.id_expression() != null ?
            ImmutableList.of(tableview_nameContext.id_expression()) : null);
    }

    private QualifiedName getQualifiedName(IdentifierContext identifierContext,
                                           List<Id_expressionContext> idExpressionContext) {
        IdentifierContext identifier = identifierContext;
        Identifier firstPart = ParserHelper.getIdentifier(identifier);
        List<Id_expressionContext> id_expressionContext = idExpressionContext;
        if (id_expressionContext != null) {
            List<Identifier> list = ParserHelper.visit(this, id_expressionContext, Identifier.class);
            return QualifiedName.of(Lists.asList(firstPart, list.toArray(new Identifier[0])));
        }
        return QualifiedName.of(ImmutableList.of(firstPart));
    }

}
