/*
 * Copyright [2024] [name of copyright owner]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aliyun.fastmodel.transform.adbpg.parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.common.parser.ParserHelper;
import com.aliyun.fastmodel.common.utils.StripUtils;
import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeParameter;
import com.aliyun.fastmodel.core.tree.datatype.NumericParameter;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.core.tree.expr.ComparisonExpression;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.atom.FunctionCall;
import com.aliyun.fastmodel.core.tree.expr.enums.ComparisonOperator;
import com.aliyun.fastmodel.core.tree.expr.enums.IntervalQualifiers;
import com.aliyun.fastmodel.core.tree.expr.literal.BooleanLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.DecimalLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.EscapeStringLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.LongLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.StringLiteral;
import com.aliyun.fastmodel.core.tree.statement.CompositeStatement;
import com.aliyun.fastmodel.core.tree.statement.constants.ConstraintType;
import com.aliyun.fastmodel.core.tree.statement.select.order.NullOrdering;
import com.aliyun.fastmodel.core.tree.statement.select.order.OrderBy;
import com.aliyun.fastmodel.core.tree.statement.select.order.Ordering;
import com.aliyun.fastmodel.core.tree.statement.select.order.SortItem;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.PartitionedBy;
import com.aliyun.fastmodel.core.tree.statement.table.TableElement;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.BaseConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.DefaultValueConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.NotNullConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.PrimaryConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.UniqueConstraint;
import com.aliyun.fastmodel.core.tree.util.IdentifierUtil;
import com.aliyun.fastmodel.transform.adbpg.parser.AdbPostgreSQLParser.A_exprContext;
import com.aliyun.fastmodel.transform.adbpg.parser.AdbPostgreSQLParser.A_expr_compareContext;
import com.aliyun.fastmodel.transform.adbpg.parser.AdbPostgreSQLParser.A_expr_typecastContext;
import com.aliyun.fastmodel.transform.adbpg.parser.AdbPostgreSQLParser.AexprconstContext;
import com.aliyun.fastmodel.transform.adbpg.parser.AdbPostgreSQLParser.Any_nameContext;
import com.aliyun.fastmodel.transform.adbpg.parser.AdbPostgreSQLParser.AnysconstContext;
import com.aliyun.fastmodel.transform.adbpg.parser.AdbPostgreSQLParser.Array_boundsContext;
import com.aliyun.fastmodel.transform.adbpg.parser.AdbPostgreSQLParser.B_exprContext;
import com.aliyun.fastmodel.transform.adbpg.parser.AdbPostgreSQLParser.Builtin_function_nameContext;
import com.aliyun.fastmodel.transform.adbpg.parser.AdbPostgreSQLParser.C_expr_exprContext;
import com.aliyun.fastmodel.transform.adbpg.parser.AdbPostgreSQLParser.CharacterContext;
import com.aliyun.fastmodel.transform.adbpg.parser.AdbPostgreSQLParser.ColconstraintContext;
import com.aliyun.fastmodel.transform.adbpg.parser.AdbPostgreSQLParser.ColconstraintelemContext;
import com.aliyun.fastmodel.transform.adbpg.parser.AdbPostgreSQLParser.ColidContext;
import com.aliyun.fastmodel.transform.adbpg.parser.AdbPostgreSQLParser.CollabelContext;
import com.aliyun.fastmodel.transform.adbpg.parser.AdbPostgreSQLParser.ColumnDefContext;
import com.aliyun.fastmodel.transform.adbpg.parser.AdbPostgreSQLParser.ColumnElemContext;
import com.aliyun.fastmodel.transform.adbpg.parser.AdbPostgreSQLParser.ColumnlistContext;
import com.aliyun.fastmodel.transform.adbpg.parser.AdbPostgreSQLParser.Comment_textContext;
import com.aliyun.fastmodel.transform.adbpg.parser.AdbPostgreSQLParser.ConstdatetimeContext;
import com.aliyun.fastmodel.transform.adbpg.parser.AdbPostgreSQLParser.ConstraintelemContext;
import com.aliyun.fastmodel.transform.adbpg.parser.AdbPostgreSQLParser.CreateforeigntablestmtContext;
import com.aliyun.fastmodel.transform.adbpg.parser.AdbPostgreSQLParser.CreatestmtContext;
import com.aliyun.fastmodel.transform.adbpg.parser.AdbPostgreSQLParser.FconstContext;
import com.aliyun.fastmodel.transform.adbpg.parser.AdbPostgreSQLParser.Func_applicationContext;
import com.aliyun.fastmodel.transform.adbpg.parser.AdbPostgreSQLParser.Func_arg_listContext;
import com.aliyun.fastmodel.transform.adbpg.parser.AdbPostgreSQLParser.Func_nameContext;
import com.aliyun.fastmodel.transform.adbpg.parser.AdbPostgreSQLParser.Generic_option_elemContext;
import com.aliyun.fastmodel.transform.adbpg.parser.AdbPostgreSQLParser.GenerictypeContext;
import com.aliyun.fastmodel.transform.adbpg.parser.AdbPostgreSQLParser.IconstContext;
import com.aliyun.fastmodel.transform.adbpg.parser.AdbPostgreSQLParser.IndirectionContext;
import com.aliyun.fastmodel.transform.adbpg.parser.AdbPostgreSQLParser.NameContext;
import com.aliyun.fastmodel.transform.adbpg.parser.AdbPostgreSQLParser.NumericContext;
import com.aliyun.fastmodel.transform.adbpg.parser.AdbPostgreSQLParser.Opt_array_boundsContext;
import com.aliyun.fastmodel.transform.adbpg.parser.AdbPostgreSQLParser.Opt_sort_clauseContext;
import com.aliyun.fastmodel.transform.adbpg.parser.AdbPostgreSQLParser.Opt_subpartition_listContext;
import com.aliyun.fastmodel.transform.adbpg.parser.AdbPostgreSQLParser.Opt_type_modifiersContext;
import com.aliyun.fastmodel.transform.adbpg.parser.AdbPostgreSQLParser.OptpartitionspecContext;
import com.aliyun.fastmodel.transform.adbpg.parser.AdbPostgreSQLParser.OpttabledistributeContext;
import com.aliyun.fastmodel.transform.adbpg.parser.AdbPostgreSQLParser.OpttableelementlistContext;
import com.aliyun.fastmodel.transform.adbpg.parser.AdbPostgreSQLParser.Part_elemContext;
import com.aliyun.fastmodel.transform.adbpg.parser.AdbPostgreSQLParser.Partition_elementContext;
import com.aliyun.fastmodel.transform.adbpg.parser.AdbPostgreSQLParser.Partition_element_with_subContext;
import com.aliyun.fastmodel.transform.adbpg.parser.AdbPostgreSQLParser.Partition_value_exprContext;
import com.aliyun.fastmodel.transform.adbpg.parser.AdbPostgreSQLParser.Partition_value_interval_exprContext;
import com.aliyun.fastmodel.transform.adbpg.parser.AdbPostgreSQLParser.PartitionspecContext;
import com.aliyun.fastmodel.transform.adbpg.parser.AdbPostgreSQLParser.Qualified_nameContext;
import com.aliyun.fastmodel.transform.adbpg.parser.AdbPostgreSQLParser.Range_elementContext;
import com.aliyun.fastmodel.transform.adbpg.parser.AdbPostgreSQLParser.Reloption_elemContext;
import com.aliyun.fastmodel.transform.adbpg.parser.AdbPostgreSQLParser.RootContext;
import com.aliyun.fastmodel.transform.adbpg.parser.AdbPostgreSQLParser.SconstContext;
import com.aliyun.fastmodel.transform.adbpg.parser.AdbPostgreSQLParser.SimpletypenameContext;
import com.aliyun.fastmodel.transform.adbpg.parser.AdbPostgreSQLParser.Sort_clauseContext;
import com.aliyun.fastmodel.transform.adbpg.parser.AdbPostgreSQLParser.SortbyContext;
import com.aliyun.fastmodel.transform.adbpg.parser.AdbPostgreSQLParser.StmtmultiContext;
import com.aliyun.fastmodel.transform.adbpg.parser.AdbPostgreSQLParser.Subpartition_elementContext;
import com.aliyun.fastmodel.transform.adbpg.parser.AdbPostgreSQLParser.Subpartition_individual_optionContext;
import com.aliyun.fastmodel.transform.adbpg.parser.AdbPostgreSQLParser.Subpartition_listContext;
import com.aliyun.fastmodel.transform.adbpg.parser.AdbPostgreSQLParser.Subpartition_template_optionContext;
import com.aliyun.fastmodel.transform.adbpg.parser.AdbPostgreSQLParser.TableconstraintContext;
import com.aliyun.fastmodel.transform.adbpg.parser.AdbPostgreSQLParser.TransactionstmtContext;
import com.aliyun.fastmodel.transform.adbpg.parser.AdbPostgreSQLParser.Type_function_nameContext;
import com.aliyun.fastmodel.transform.adbpg.parser.AdbPostgreSQLParser.TypenameContext;
import com.aliyun.fastmodel.transform.adbpg.parser.tree.AdbPostgreSQLPartitionBy;
import com.aliyun.fastmodel.transform.adbpg.parser.tree.partition.desc.AdbPostgreSQLRangeElement;
import com.aliyun.fastmodel.transform.adbpg.parser.tree.partition.desc.PartitionIntervalExpression;
import com.aliyun.fastmodel.transform.adbpg.parser.tree.partition.desc.PartitionValueExpression;
import com.aliyun.fastmodel.transform.api.context.ReverseContext;
import com.aliyun.fastmodel.transform.api.extension.client.property.ExtensionPropertyKey;
import com.aliyun.fastmodel.transform.api.extension.tree.constraint.CheckExpressionConstraint;
import com.aliyun.fastmodel.transform.api.extension.tree.constraint.desc.DistributeNonKeyConstraint;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.PartitionByType;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.desc.PartitionDesc;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.sub.BaseSubPartition;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.sub.SubListPartition;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.sub.SubRangePartition;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.sub.element.BasePartitionElement;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.sub.element.BaseSubPartitionElement;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.sub.element.ListPartitionElement;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.sub.element.RangePartitionElement;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.sub.element.SubListPartitionElement;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.sub.element.SubPartitionList;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.sub.element.SubRangePartitionElement;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.sub.template.SubListTemplatePartition;
import com.aliyun.fastmodel.transform.api.extension.tree.partition.sub.template.SubRangeTemplatePartition;
import com.aliyun.fastmodel.transform.postgresql.client.property.PostgreSQLPropertyKey;
import com.aliyun.fastmodel.transform.postgresql.parser.tree.BeginWork;
import com.aliyun.fastmodel.transform.postgresql.parser.tree.CommitWork;
import com.aliyun.fastmodel.transform.postgresql.parser.tree.datatype.ArrayBounds;
import com.aliyun.fastmodel.transform.postgresql.parser.tree.datatype.PostgreSQLArrayDataType;
import com.aliyun.fastmodel.transform.postgresql.parser.tree.datatype.PostgreSQLDataTypeName;
import com.aliyun.fastmodel.transform.postgresql.parser.tree.datatype.PostgreSQLGenericDataType;
import com.aliyun.fastmodel.transform.postgresql.parser.tree.datatype.PostgreSQLIntervalDataType;
import com.aliyun.fastmodel.transform.postgresql.parser.tree.datatype.PostgreSQLRowDataType;
import com.aliyun.fastmodel.transform.postgresql.parser.tree.datatype.PostgreSQLRowDataType.RowType;
import com.aliyun.fastmodel.transform.postgresql.parser.tree.expr.WithDataTypeNameExpression;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * ast builder
 *
 * @author panguanjing
 * @date 2024/10/13
 */
public class AdbPostgreSQLAstBuilder extends AdbPostgreSQLParserBaseVisitor<Node> {

    private final ReverseContext reverseContext;

    public AdbPostgreSQLAstBuilder(ReverseContext context) {
        this.reverseContext = context;
    }

    @Override
    public Node visitRoot(RootContext ctx) {
        return visit(ctx.stmtblock());
    }

    @Override
    public Node visitStmtmulti(StmtmultiContext ctx) {
        List<BaseStatement> visit = ParserHelper.visit(this, ctx.stmt(), BaseStatement.class);
        if (CollectionUtils.isEmpty(visit)) {
            //ignore
            return null;
        }
        if (visit.size() > 1) {
            return new CompositeStatement(visit);
        } else {
            return visit.get(0);
        }
    }

    @Override
    public Node visitTransactionstmt(TransactionstmtContext ctx) {
        if (ctx.BEGIN_P() != null) {
            return new BeginWork();
        }
        if (ctx.COMMIT() != null) {
            return new CommitWork();
        }
        return super.visitTransactionstmt(ctx);
    }

    @Override
    public Node visitTypename(TypenameContext ctx) {
        SimpletypenameContext simpletypename = ctx.simpletypename();
        if (simpletypename != null) {
            BaseDataType baseDataType = (BaseDataType)visit(simpletypename);
            if (ctx.opt_array_bounds() != null) {
                List<Array_boundsContext> contexts = ctx.opt_array_bounds().array_bounds();
                List<ArrayBounds> list = ParserHelper.visit(this, contexts, ArrayBounds.class);
                if (CollectionUtils.isNotEmpty(list)) {
                    return new PostgreSQLArrayDataType(baseDataType, list);
                }
                return baseDataType;
            } else if (ctx.ARRAY() != null) {
                if (ctx.iconst() == null) {
                    return new PostgreSQLArrayDataType(baseDataType, null);
                } else {
                    LongLiteral longLiteral = (LongLiteral)visit(ctx.iconst());
                    ArrayBounds arrayBounds = new ArrayBounds(longLiteral.getValue().intValue());
                    return new PostgreSQLArrayDataType(baseDataType, Lists.newArrayList(arrayBounds));
                }
            } else {
                return baseDataType;
            }
        }
        if (ctx.qualified_name() != null) {
            QualifiedName qualifiedName = (QualifiedName)visit(ctx.qualified_name());
            if (ctx.ROWTYPE() != null) {
                return new PostgreSQLRowDataType(qualifiedName, RowType.ROWTYPE);
            }
            if (ctx.TYPE_P() != null) {
                return new PostgreSQLRowDataType(qualifiedName, RowType.TYPE);
            }
        }
        return null;
    }

    @Override
    public Node visitArray_bounds(Array_boundsContext ctx) {
        boolean iconsNull = ctx.iconst() == null;
        if (iconsNull) {
            return new ArrayBounds(null);
        } else {
            LongLiteral longLiteral = (LongLiteral)visit(ctx.iconst());
            return new ArrayBounds(longLiteral.getValue().intValue());
        }
    }

    @Override
    public Node visitCreatestmt(CreatestmtContext ctx) {
        QualifiedName qualifiedName = (QualifiedName)visit(ctx.qualified_name(0));
        boolean isNotExist = ctx.EXISTS() != null && ctx.NOT() != null;
        List<BaseConstraint> constraints = null;
        List<ColumnDefinition> columnDefinitions = null;
        if (ctx.opttableelementlist() != null) {
            List<TableElement> visit = ParserHelper.visit(this, ctx.opttableelementlist().tableelementlist().tableelement(), TableElement.class);
            columnDefinitions = visit.stream().filter(t -> t instanceof ColumnDefinition).map(t -> {
                return (ColumnDefinition)t;
            }).collect(Collectors.toList());

            constraints = visit.stream().filter(t -> t instanceof BaseConstraint).map(t -> {
                return (BaseConstraint)t;
            }).collect(Collectors.toList());
        }
        PartitionedBy partitionedBy = null;
        if (ctx.optpartitionspec() != null && ctx.optpartitionspec().getChildCount() > 0 && columnDefinitions != null) {
            partitionedBy = (PartitionedBy)visit(ctx.optpartitionspec());
        }

        if (ctx.opttabledistribute() != null && ctx.opttabledistribute().getChildCount() > 0) {
            DistributeNonKeyConstraint distributeNonKeyConstraint = (DistributeNonKeyConstraint)visit(ctx.opttabledistribute());
            if (constraints != null) {
                constraints.add(distributeNonKeyConstraint);
            } else {
                constraints = Lists.newArrayList(distributeNonKeyConstraint);
            }
        }

        List<Property> properties = buildProperties(ctx);
        return CreateTable.builder()
            .ifNotExist(isNotExist)
            .columns(columnDefinitions)
            .tableName(qualifiedName)
            .constraints(constraints)
            .partition(partitionedBy)
            .properties(properties)
            .build();
    }

    @Override
    public Node visitOpttabledistribute(OpttabledistributeContext ctx) {
        if(ctx.RANDOMLY() != null) {
            return new DistributeNonKeyConstraint(null, true, null, null);
        } else if (ctx.REPLICATED() != null) {
            return new DistributeNonKeyConstraint(null, null, true, null);
        } else {
            List<Identifier> columns = ParserHelper.visit(this, ctx.opt_column_list().columnlist().columnElem(), Identifier.class);
            return new DistributeNonKeyConstraint(columns, null);
        }
    }

    @Override
    public Node visitCreateforeigntablestmt(CreateforeigntablestmtContext ctx) {
        QualifiedName qualifiedName = (QualifiedName)visit(ctx.qualified_name(0));
        boolean isNotExist = ctx.EXISTS() != null && ctx.NOT() != null;
        OpttableelementlistContext opttableelementlist = ctx.opttableelementlist();
        List<ColumnDefinition> columnDefinitions = null;
        List<BaseConstraint> constraints = null;
        if (opttableelementlist != null) {
            List<TableElement> visit = ParserHelper.visit(this, opttableelementlist.tableelementlist().tableelement(), TableElement.class);
            columnDefinitions = visit.stream().filter(t -> t instanceof ColumnDefinition).map(t -> (ColumnDefinition)t).collect(Collectors.toList());

            constraints = visit.stream().filter(t -> t instanceof BaseConstraint).map(t -> (BaseConstraint)t).collect(Collectors.toList());
        }

        List<Property> properties = new ArrayList<>();
        if (ctx.FOREIGN() != null) {
            properties.add(new Property(PostgreSQLPropertyKey.FOREIGN.getValue(), new BooleanLiteral(BooleanLiteral.TRUE)));
        }

        // server
        if (ctx.SERVER() != null) {
            Identifier serverName = (Identifier)visit(ctx.name().colid());
            properties.add(new Property(PostgreSQLPropertyKey.SERVER_NAME.getValue(), serverName.getValue()));
        }

        //options
        List<Property> options = buildOptions(ctx);
        properties.addAll(options);

        return CreateTable.builder().ifNotExist(isNotExist).columns(columnDefinitions).tableName(qualifiedName).constraints(constraints).properties(
            properties).build();
    }

    @Override
    public Property visitReloption_elem(Reloption_elemContext ctx) {
        String key = ctx.collabel().get(0).identifier().Identifier().getText();
        String value = ctx.def_arg().getText();
        return new Property(key, StripUtils.strip(value));
    }

    private PartitionedBy mapColumn(PartitionedBy partitionedBy, List<ColumnDefinition> columnDefinitions) {
        Map<Identifier, ColumnDefinition> map = Maps.newHashMap();
        for (ColumnDefinition columnDefinition : columnDefinitions) {
            map.put(columnDefinition.getColName(), columnDefinition);
        }
        List<ColumnDefinition> columnDefinitions1 = partitionedBy.getColumnDefinitions();
        List<ColumnDefinition> partitionColumns = Lists.newArrayList();
        for (ColumnDefinition p : columnDefinitions1) {
            ColumnDefinition columnDefinition = map.get(p.getColName());
            partitionColumns.add(columnDefinition);
        }
        return new PartitionedBy(partitionColumns);
    }

    @Override
    public Node visitOptpartitionspec(OptpartitionspecContext ctx) {
        return visit(ctx.partitionspec());
    }

    @Override
    public Node visitPartitionspec(PartitionspecContext ctx) {
        List<Identifier> list = ParserHelper.visit(this, ctx.part_params().part_elem(), Identifier.class);
        PartitionByType partitionByType = null;
        TerminalNode terminalNode1 = ctx.RANGE();
        if (terminalNode1 != null) {
            //partition range
            partitionByType = PartitionByType.RANGE;
        }
        if (ctx.BISON_LIST() != null) {
            //partition list
            partitionByType = PartitionByType.LIST;
        }
        List<ColumnDefinition> collect = list.stream().map(c -> ColumnDefinition.builder().colName(c).build()).collect(Collectors.toList());
        List<BaseSubPartition> subPartitions = null;
        if (ctx.subpartition_option() != null) {
            subPartitions = ParserHelper.visit(this, ctx.subpartition_option(), BaseSubPartition.class);
        }
        List<BasePartitionElement> partitionElements = null;
        if (ctx.partition_element_list() != null) {
            partitionElements = ParserHelper.visit(this, ctx.partition_element_list().partition_element_with_sub(), BasePartitionElement.class);
        }
        return new AdbPostgreSQLPartitionBy(collect, partitionByType, subPartitions, partitionElements);
    }

    @Override
    public Node visitPartition_element_with_sub(Partition_element_with_subContext ctx) {

        return super.visitPartition_element_with_sub(ctx);
    }

    @Override
    public Node visitPartition_element(Partition_elementContext ctx) {
        QualifiedName qualifiedName = null;
        if (ctx.colid() != null) {
            Identifier identifier = (Identifier)visit(ctx.colid());
            qualifiedName = QualifiedName.of(Lists.newArrayList(identifier));
        }
        if (ctx.DEFAULT() != null) {
            return new ListPartitionElement(qualifiedName, true, null,
                null, null, null);
        }
        // sub list partition element
        SubPartitionList subPartitionList = null;
        if (ctx.opt_subpartition_list() != null) {
            subPartitionList = (SubPartitionList)visit(ctx.opt_subpartition_list());
        }
        if (ctx.list_element() != null) {
            List<BaseExpression> expressionList = ParserHelper.visit(this, ctx.list_element().list_expr().a_expr(), BaseExpression.class);
            return new ListPartitionElement(qualifiedName,
                false, expressionList, null, null, subPartitionList);
        }
        //sub range partition element
        if (ctx.range_element() != null) {
            PartitionDesc partitionDesc = (PartitionDesc)visit(ctx.range_element());
            return new RangePartitionElement(qualifiedName, null, partitionDesc, subPartitionList);
        }
        return null;
    }

    @Override
    public Node visitSubpartition_template_option(Subpartition_template_optionContext ctx) {
        BaseExpression baseExpression = (BaseExpression)visit(ctx.a_expr());
        SubPartitionList subPartitionList = null;
        if (ctx.opt_subpartition_list() != null) {
            subPartitionList = (SubPartitionList)visit(ctx.opt_subpartition_list());
        }
        if (ctx.RANGE() != null) {
            return new SubRangeTemplatePartition(
                baseExpression,
                null,
                subPartitionList
            );
        }
        if (ctx.BISON_LIST() != null) {
            return new SubListTemplatePartition(
                baseExpression, null, subPartitionList
            );
        }
        return null;
    }

    @Override
    public Node visitSubpartition_individual_option(Subpartition_individual_optionContext ctx) {
        BaseExpression baseExpression = (BaseExpression)visit(ctx.a_expr());
        if (ctx.RANGE() != null) {
            return new SubRangePartition(baseExpression, null, null);
        }
        if (ctx.BISON_LIST() != null) {
            return new SubListPartition(baseExpression, null);
        }
        return null;
    }

    @Override
    public Node visitOpt_subpartition_list(Opt_subpartition_listContext ctx) {
        if (ctx.subpartition_list() != null) {
            return visit(ctx.subpartition_list());
        }
        return null;
    }

    @Override
    public Node visitSubpartition_list(Subpartition_listContext ctx) {
        List<BaseSubPartitionElement> subPartitionElements =
            ParserHelper.visit(this, ctx.subpartition_element(), BaseSubPartitionElement.class);
        return new SubPartitionList(subPartitionElements);
    }

    @Override
    public Node visitSubpartition_element(Subpartition_elementContext ctx) {
        QualifiedName qualifiedName = null;
        if (ctx.colid() != null) {
            Identifier identifier = (Identifier)visit(ctx.colid());
            qualifiedName = QualifiedName.of(Lists.newArrayList(identifier));
        }
        if (ctx.DEFAULT() != null) {
            return new SubListPartitionElement(qualifiedName, true, null, null);
        }
        // sub list partition element
        if (ctx.list_element() != null) {
            List<BaseExpression> expressionList = ParserHelper.visit(this, ctx.list_element().list_expr().a_expr(), BaseExpression.class);
            return new SubListPartitionElement(qualifiedName, false, expressionList, null);
        }
        //sub range partition element
        if (ctx.range_element() != null) {
            PartitionDesc partitionDesc = (PartitionDesc)visit(ctx.range_element());
            return new SubRangePartitionElement(partitionDesc, qualifiedName);
        }
        return super.visitSubpartition_element(ctx);
    }

    @Override
    public Node visitRange_element(Range_elementContext ctx) {
        PartitionValueExpression start = null;
        if (ctx.start != null) {
            start = (PartitionValueExpression)visit(ctx.start);
        }
        PartitionValueExpression end = null;
        if (ctx.end != null) {
            end = (PartitionValueExpression)visit(ctx.end);
        }
        PartitionIntervalExpression every = null;
        if (ctx.every != null) {
            every = (PartitionIntervalExpression)visit(ctx.every);
        }
        return new AdbPostgreSQLRangeElement(start, end, every);
    }

    @Override
    public Node visitPartition_value_interval_expr(Partition_value_interval_exprContext ctx) {
        BaseDataType baseDataType = null;
        if (ctx.typename() != null) {
            baseDataType = (BaseDataType)visit(ctx.typename());
        }
        BaseExpression baseExpression = null;
        if (ctx.a_expr() != null) {
            baseExpression = (BaseExpression)visit(ctx.a_expr());
        }
        StringLiteral stringLiteral = null;
        if (ctx.StringConstant() != null) {
            stringLiteral = new StringLiteral(StripUtils.strip(ctx.StringConstant().getText()));
        }
        return new PartitionIntervalExpression(baseDataType, baseExpression, stringLiteral);
    }

    @Override
    public Node visitPartition_value_expr(Partition_value_exprContext ctx) {
        BaseDataType baseDataType = null;
        if (ctx.typename() != null) {
            baseDataType = (BaseDataType)visit(ctx.typename());
        }
        BaseExpression baseExpression = null;
        if (ctx.a_expr() != null) {
            baseExpression = (BaseExpression)visit(ctx.a_expr());
        }
        return new PartitionValueExpression(baseDataType, baseExpression);
    }

    @Override
    public Node visitPart_elem(Part_elemContext ctx) {
        return visit(ctx.colid());
    }

    @Override
    public Node visitColumnDef(ColumnDefContext ctx) {
        Identifier identifier = (Identifier)visit(ctx.colid());
        BaseDataType baseDataType = (BaseDataType)visit(ctx.typename());
        List<BaseConstraint> inlineConstraint = ImmutableList.of();
        if (ctx.colquallist() != null) {
            inlineConstraint = ParserHelper.visit(this, ctx.colquallist().colconstraint(), BaseConstraint.class);
        }
        List<Property> properties = Lists.newArrayList();
        //check index
        BaseConstraint checkConstraint = inlineConstraint.stream().filter(c -> {
            return c instanceof CheckExpressionConstraint;
        }).findFirst().orElse(null);
        if (checkConstraint != null) {
            CheckExpressionConstraint constraint = (CheckExpressionConstraint)checkConstraint;
            String origin = constraint.getExpression().getOrigin();
            properties.add(new Property(ExtensionPropertyKey.COLUMN_CHECK.getValue(), origin));
        }
        return ColumnDefinition.builder()
            .colName(identifier)
            .dataType(baseDataType)
            .notNull(toNotNull(inlineConstraint))
            .primary(toPrimary(inlineConstraint))
            .defaultValue(toDefaultValue(inlineConstraint))
            .properties(properties)
            .build();
    }

    @Override
    public Node visitColconstraint(ColconstraintContext ctx) {
        Identifier identifier = ParserHelper.visitIfPresent(this, ctx.name(), Identifier.class).orElse(null);
        BaseConstraint baseConstraint = ParserHelper.visitIfPresent(this, ctx.colconstraintelem(), BaseConstraint.class).orElse(null);
        if (baseConstraint == null) {
            return null;
        }
        if (identifier != null) {
            baseConstraint.setName(identifier);
        }
        return baseConstraint;
    }

    @Override
    public Node visitColconstraintelem(ColconstraintelemContext ctx) {
        if (ctx.NULL_P() != null) {
            return new NotNullConstraint(IdentifierUtil.sysIdentifier(), ctx.NOT() != null);
        }
        if (ctx.PRIMARY() != null) {
            return new PrimaryConstraint(IdentifierUtil.sysIdentifier(), ImmutableList.of());
        }
        if (ctx.DEFAULT() != null) {
            BaseExpression baseExpression = (BaseExpression)visit(ctx.b_expr());
            return new DefaultValueConstraint(IdentifierUtil.sysIdentifier(), baseExpression);
        }
        if (ctx.CHECK() != null) {
            BaseExpression expression = (BaseExpression)visit(ctx.a_expr());
            return new CheckExpressionConstraint(IdentifierUtil.sysIdentifier(), expression, false);
        }
        return super.visitColconstraintelem(ctx);
    }

    @Override
    public Node visitA_expr_compare(A_expr_compareContext ctx) {
        ComparisonOperator operator = getOperator(ctx);
        if (operator == null) {
            return super.visitA_expr_compare(ctx);
        }
        BaseExpression left = null;
        BaseExpression right = null;
        if (ctx.a_expr_like() != null && ctx.a_expr_like().size() == 2) {
            left = (BaseExpression)visit(ctx.a_expr_like(0));
            right = (BaseExpression)visit(ctx.a_expr_like(1));
        } else {
            return super.visitA_expr_compare(ctx);
        }
        return new ComparisonExpression(ParserHelper.getLocation(ctx), ParserHelper.getOrigin(ctx), operator, left, right);
    }

    private ComparisonOperator getOperator(A_expr_compareContext ctx) {
        if (ctx.LT() != null) {
            return ComparisonOperator.LESS_THAN;
        }
        if (ctx.EQUAL() != null) {
            return ComparisonOperator.EQUAL;
        }
        if (ctx.GT() != null) {
            return ComparisonOperator.GREATER_THAN;
        }
        if (ctx.LESS_EQUALS() != null) {
            return ComparisonOperator.LESS_THAN_OR_EQUAL;
        }
        if (ctx.GREATER_EQUALS() != null) {
            return ComparisonOperator.GREATER_THAN_OR_EQUAL;
        }
        if (ctx.NOT_EQUALS() != null) {
            return ComparisonOperator.NOT_EQUAL;
        }
        return null;
    }

    @Override
    public Node visitQualified_name(Qualified_nameContext ctx) {
        return getQualifiedName(ctx.colid(), ctx.indirection());
    }

    @Override
    public Node visitAexprconst(AexprconstContext ctx) {
        if (ctx.FALSE_P() != null) {
            return new BooleanLiteral("FALSE");
        }
        if (ctx.TRUE_P() != null) {
            return new BooleanLiteral("TRUE");
        }
        return super.visitAexprconst(ctx);
    }

    private QualifiedName getQualifiedName(ColidContext context, IndirectionContext indirectionContext) {
        Identifier identifier = (Identifier)visit(context);
        if (indirectionContext != null) {
            List<Identifier> identifiers = ParserHelper.visit(this, indirectionContext.indirection_el(), Identifier.class);
            List<Identifier> all = Lists.newArrayList(identifier);
            all.addAll(identifiers);
            return QualifiedName.of(all);
        }
        return QualifiedName.of(Arrays.asList(identifier));
    }

    @Override
    public Node visitCollabel(CollabelContext ctx) {
        return ParserHelper.getIdentifier(ctx);
    }

    @Override
    public Node visitName(NameContext ctx) {
        return visit(ctx.colid());
    }

    @Override
    public Node visitFconst(FconstContext ctx) {
        return new DecimalLiteral(ctx.getText());
    }

    @Override
    public Node visitIconst(IconstContext ctx) {
        return new LongLiteral(ctx.getText());
    }

    @Override
    public Node visitColid(ColidContext ctx) {
        return ParserHelper.getIdentifier(ctx);
    }

    @Override
    public Node visitTableconstraint(TableconstraintContext ctx) {
        NameContext name = ctx.name();
        Identifier identifier = ParserHelper.visitIfPresent(this, name, Identifier.class).orElse(null);
        BaseConstraint baseConstraint = (BaseConstraint)visit(ctx.constraintelem());
        if (identifier != null) {
            baseConstraint.setName(identifier);
        }
        return baseConstraint;
    }

    @Override
    public Node visitConstraintelem(ConstraintelemContext ctx) {
        if (ctx.PRIMARY() != null) {
            ColumnlistContext columnlist = ctx.columnlist();
            List<Identifier> list = ParserHelper.visit(this, columnlist.columnElem(), Identifier.class);
            return new PrimaryConstraint(IdentifierUtil.sysIdentifier(), list);
        }
        if (ctx.UNIQUE() != null) {
            ColumnlistContext columnlist = ctx.columnlist();
            List<Identifier> list = ParserHelper.visit(this, columnlist.columnElem(), Identifier.class);
            return new UniqueConstraint(IdentifierUtil.sysIdentifier(), list);
        }
        //un support other constraint
        return null;
    }

    @Override
    public Node visitColumnElem(ColumnElemContext ctx) {
        return visit(ctx.colid());
    }

    @Override
    public Node visitFunc_application(Func_applicationContext ctx) {
        QualifiedName functionName = (QualifiedName)visit(ctx.func_name());
        List<BaseExpression> list = ImmutableList.of();
        Func_arg_listContext func_arg_listContext = ctx.func_arg_list();
        OrderBy orderBy = null;
        if (func_arg_listContext != null) {
            list = ParserHelper.visit(this, func_arg_listContext.func_arg_expr(), BaseExpression.class);
        } else if (ctx.opt_sort_clause() != null) {
            orderBy = (OrderBy)visit(ctx.opt_sort_clause());
        }
        FunctionCall functionCall = new FunctionCall(functionName, ctx.DISTINCT() != null, list, null, null, null, orderBy);
        return functionCall;
    }

    @Override
    public Node visitA_expr_typecast(A_expr_typecastContext ctx) {
        BaseExpression expression = (BaseExpression)visit(ctx.c_expr());
        BaseDataType baseDataType = null;
        if (CollectionUtils.isNotEmpty(ctx.typename())) {
            baseDataType = (BaseDataType)visit(ctx.typename(0));
        } else {
            return expression;
        }
        return new WithDataTypeNameExpression(ParserHelper.getLocation(ctx), ParserHelper.getOrigin(ctx), expression, baseDataType);
    }

    @Override
    public Node visitB_expr(B_exprContext ctx) {
        if (ctx.c_expr() != null && ctx.c_expr() instanceof C_expr_exprContext) {
            C_expr_exprContext exprExprContext = (C_expr_exprContext)ctx.c_expr();
            if (exprExprContext.func_expr() != null && exprExprContext.func_expr().func_application() != null) {
                return visit(exprExprContext.func_expr().func_application());
            }
            if (exprExprContext.columnref() != null) {
                return visit(exprExprContext.columnref());
            }
        }
        if (ctx.typename() != null) {
            BaseExpression baseExpression = (BaseExpression)visit(ctx.b_expr(0));
            BaseDataType dataTypeName = (BaseDataType)visit(ctx.typename());
            return new WithDataTypeNameExpression(ParserHelper.getLocation(ctx), ParserHelper.getOrigin(ctx), baseExpression, dataTypeName);
        }
        return visitChildren(ctx);
    }

    @Override
    public Node visitOpt_sort_clause(Opt_sort_clauseContext ctx) {
        return visit(ctx.sort_clause());
    }

    @Override
    public Node visitSort_clause(Sort_clauseContext ctx) {
        List<SortItem> list = ParserHelper.visit(this, ctx.sortby_list().sortby(), SortItem.class);
        return new OrderBy(list);
    }

    @Override
    public Node visitSortby(SortbyContext ctx) {
        BaseExpression baseExpression = (BaseExpression)visit(ctx.a_expr());
        Ordering ordering = null;
        if (StringUtils.isNotBlank(ctx.opt_asc_desc().getText())) {
            ordering = Ordering.getByCode(ctx.opt_asc_desc().getText());
        }
        NullOrdering nullOrdering = null;
        if (StringUtils.isNotBlank(ctx.opt_nulls_order().getText())) {
            nullOrdering = NullOrdering.getByCode(ctx.opt_nulls_order().getText());
        }
        SortItem sortItem = new SortItem(baseExpression, ordering, nullOrdering);
        return sortItem;
    }

    @Override
    public Node visitFunc_name(Func_nameContext ctx) {
        if (ctx.colid() != null) {
            return getQualifiedName(ctx.colid(), ctx.indirection());
        }
        if (ctx.type_function_name() != null) {
            Identifier identifier = (Identifier)visit(ctx.type_function_name());
            return QualifiedName.of(Lists.newArrayList(identifier));
        }
        if (ctx.builtin_function_name() != null) {
            Identifier identifier = (Identifier)visit(ctx.builtin_function_name());
            return QualifiedName.of(Lists.newArrayList(identifier));
        }
        if (ctx.LEFT() != null) {
            return QualifiedName.of(ctx.LEFT().getText());
        }
        if (ctx.RIGHT() != null) {
            return QualifiedName.of(ctx.RIGHT().getText());
        }
        return null;
    }

    @Override
    public Node visitBuiltin_function_name(Builtin_function_nameContext ctx) {
        return ParserHelper.getIdentifier(ctx);
    }

    @Override
    public Node visitType_function_name(Type_function_nameContext ctx) {
        return ParserHelper.getIdentifier(ctx);
    }

    @Override
    public Node visitSconst(SconstContext ctx) {
        StringLiteral visit = (StringLiteral)visit(ctx.anysconst());
        if (ctx.opt_uescape() == null || ctx.opt_uescape().getChildCount() == 0) {
            return visit;
        }
        StringLiteral stringLiteral = (StringLiteral)visit(ctx.opt_uescape().anysconst());
        return new EscapeStringLiteral(visit.getValue(), stringLiteral.getValue());
    }

    @Override
    public Node visitSimpletypename(SimpletypenameContext ctx) {
        if (ctx.constinterval() != null) {
            String name = PostgreSQLDataTypeName.INTERVAL.getValue();
            if (ctx.OPEN_PAREN() != null) {
                List<DataTypeParameter> list = Lists.newArrayList();
                LongLiteral visit = (LongLiteral)visit(ctx.iconst());
                DataTypeParameter dataTypeParameter = new NumericParameter(visit.getValue().toString());
                list.add(dataTypeParameter);
                return new PostgreSQLGenericDataType(name, list);
            } else {
                IntervalQualifiers from = null;
                IntervalQualifiers to = null;
                if (ctx.opt_interval() != null) {
                    Token from1 = ctx.opt_interval().from;
                    if (from1 != null) {
                        from = IntervalQualifiers.getIntervalQualifiers(from1.getText());
                    }
                    Token token = ctx.opt_interval().to;
                    if (token != null) {
                        to = IntervalQualifiers.getIntervalQualifiers(token.getText());
                    }
                }
                return new PostgreSQLIntervalDataType(
                    from, to
                );
            }
        } else {
            return super.visitSimpletypename(ctx);
        }
    }

    @Override
    public Node visitOpt_array_bounds(Opt_array_boundsContext ctx) {
        return super.visitOpt_array_bounds(ctx);
    }

    @Override
    public Node visitAnysconst(AnysconstContext ctx) {
        return new StringLiteral(StripUtils.strip(ctx.StringConstant().getText()));
    }

    @Override
    public Node visitGenerictype(GenerictypeContext ctx) {
        Opt_type_modifiersContext opt_type_modifiersContext = ctx.opt_type_modifiers();
        List<DataTypeParameter> list = ImmutableList.of();
        if (opt_type_modifiersContext != null && opt_type_modifiersContext.expr_list() != null) {
            list = getDataTypeParameters(opt_type_modifiersContext.expr_list().a_expr());
        }
        String text = ctx.type_function_name().getText();
        return new PostgreSQLGenericDataType(text, list);
    }

    @Override
    public Node visitNumeric(NumericContext ctx) {
        String text = ctx.dataTypeName.getText();
        if (StringUtils.equalsIgnoreCase(text, DataTypeEnums.DOUBLE.getName())) {
            return new PostgreSQLGenericDataType(PostgreSQLDataTypeName.DOUBLE_PRECISION.getValue());
        }
        if (StringUtils.equalsIgnoreCase(text, DataTypeEnums.DECIMAL.name()) || StringUtils.equalsIgnoreCase(text,
            PostgreSQLDataTypeName.DECIMAL.getAlias())) {
            Opt_type_modifiersContext opt_type_modifiersContext = ctx.opt_type_modifiers();
            if (opt_type_modifiersContext != null && opt_type_modifiersContext.expr_list() != null) {
                List<A_exprContext> aExprContexts = opt_type_modifiersContext.expr_list().a_expr();
                List<DataTypeParameter> list = getDataTypeParameters(aExprContexts);
                return new PostgreSQLGenericDataType(PostgreSQLDataTypeName.DECIMAL.getValue(), list);
            } else {
                return new PostgreSQLGenericDataType(PostgreSQLDataTypeName.DECIMAL.getValue());
            }
        }
        return new PostgreSQLGenericDataType(text);
    }

    private List<DataTypeParameter> getDataTypeParameters(List<A_exprContext> a_exprContexts) {
        List<DataTypeParameter> list = new ArrayList<>();
        for (A_exprContext a : a_exprContexts) {
            BaseExpression baseExpression = (BaseExpression)visit(a);
            if (baseExpression instanceof LongLiteral) {
                LongLiteral decimalLiteral = (LongLiteral)baseExpression;
                list.add(new NumericParameter(decimalLiteral.getValue().toString()));
            }
        }
        return list;
    }

    @Override
    public Node visitCharacter(CharacterContext ctx) {
        DataTypeParameter typeParameter = null;
        if (ctx.iconst() != null) {
            LongLiteral longLiteral = (LongLiteral)visit(ctx.iconst());
            typeParameter = new NumericParameter(longLiteral.getValue().toString());
        }
        return new PostgreSQLGenericDataType(ctx.character_c().getText(),
            typeParameter == null ? Collections.emptyList() : Lists.newArrayList(typeParameter));
    }

    @Override
    public Node visitConstdatetime(ConstdatetimeContext ctx) {
        DataTypeParameter typeParameter = null;
        if (ctx.iconst() != null) {
            LongLiteral longLiteral = (LongLiteral)visit(ctx.iconst());
            typeParameter = new NumericParameter(longLiteral.getValue().toString());
        }
        String name = ctx.TIME() != null ? ctx.TIME().getText() : ctx.TIMESTAMP().getText();
        List<DataTypeParameter> arguments = typeParameter == null ? Collections.emptyList() : Lists.newArrayList(typeParameter);
        if (ctx.opt_timezone() == null || ctx.opt_timezone().getChildCount() == 0) {
            return new PostgreSQLGenericDataType(name, arguments);
        }
        boolean without = ctx.opt_timezone().WITHOUT() != null;
        if (without) {
            throw new UnsupportedOperationException("unsupported without zone dataType");
        } else {
            return new PostgreSQLGenericDataType(PostgreSQLDataTypeName.TIMESTAMPTZ.getValue(), arguments);
        }
    }

    @Override
    public Node visitComment_text(Comment_textContext ctx) {
        if (ctx.NULL_P() != null) {
            return new Comment(null);
        }
        return new Comment(StripUtils.strip(ctx.getText()));
    }

    @Override
    public Node visitGeneric_option_elem(Generic_option_elemContext ctx) {
        String key = ctx.generic_option_name().collabel().identifier().Identifier().toString();
        String value = ctx.generic_option_arg().sconst().anysconst().StringConstant().toString();

        return new Property(key, StripUtils.strip(value));
    }

    @Override
    public Node visitAny_name(Any_nameContext ctx) {
        List<Identifier> list = Lists.newArrayList();
        Identifier identifier = (Identifier)visit(ctx.colid());
        list.add(identifier);
        if (ctx.attrs() != null) {
            List<Identifier> others = ParserHelper.visit(this, ctx.attrs().attr_name(), Identifier.class);
            list.addAll(others);
        }
        return QualifiedName.of(list);
    }

    private Boolean toPrimary(List<BaseConstraint> inlineConstraint) {
        Optional<BaseConstraint> first = inlineConstraint.stream().filter(c -> {
            return c.getConstraintType() == ConstraintType.PRIMARY_KEY;
        }).findFirst();
        return first.map(BaseConstraint::getEnable).orElse(null);
    }

    private Boolean toNotNull(List<BaseConstraint> inlineConstraint) {
        Optional<BaseConstraint> first = inlineConstraint.stream().filter(c -> {
            return c.getConstraintType() == ConstraintType.NOT_NULL;
        }).findFirst();
        return first.map(BaseConstraint::getEnable).orElse(null);
    }

    private BaseExpression toDefaultValue(List<BaseConstraint> inlineConstraint) {
        Optional<BaseConstraint> first = inlineConstraint.stream().filter(c -> {
            return c.getConstraintType() == ConstraintType.DEFAULT_VALUE;
        }).findFirst();
        if (first.isEmpty()) {
            return null;
        }
        DefaultValueConstraint defaultValueConstraint = (DefaultValueConstraint)first.get();
        return defaultValueConstraint.getValue();
    }

    private List<Property> buildProperties(CreatestmtContext ctx) {
        if (ctx.optwith() == null) {
            return Collections.emptyList();
        }
        if (ctx.optwith().reloptions() == null) {
            return Collections.emptyList();
        }
        if (ctx.optwith().reloptions().reloption_list() == null) {
            return Collections.emptyList();
        }
        if (CollectionUtils.isEmpty(ctx.optwith().reloptions().reloption_list().reloption_elem())) {
            return Collections.emptyList();
        }

        List<Property> properties = new ArrayList<>();
        List<Reloption_elemContext> reloptionElemContexts = ctx.optwith().reloptions().reloption_list().reloption_elem();
        reloptionElemContexts.forEach(reloptionElemContext -> {
            Property property = (Property)visit(reloptionElemContext);
            properties.add(property);
        });
        return properties;
    }

    private List<Property> buildOptions(CreateforeigntablestmtContext ctx) {
        if (ctx.create_generic_options() == null) {
            return Collections.emptyList();
        }
        if (ctx.create_generic_options().generic_option_list() == null) {
            return Collections.emptyList();
        }
        if (CollectionUtils.isEmpty(ctx.create_generic_options().generic_option_list().generic_option_elem())) {
            return Collections.emptyList();
        }

        List<Property> options = new ArrayList<>();
        List<Generic_option_elemContext> genericOptionElemContexts = ctx.create_generic_options().generic_option_list().generic_option_elem();
        genericOptionElemContexts.forEach(genericOptionElemContext -> {
            Property property = (Property)visit(genericOptionElemContext);
            options.add(property);
        });
        return options;
    }
}
