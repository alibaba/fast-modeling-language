/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.hologres.parser;

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
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.expr.atom.FunctionCall;
import com.aliyun.fastmodel.core.tree.expr.literal.BooleanLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.DecimalLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.EscapeStringLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.LongLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.StringLiteral;
import com.aliyun.fastmodel.core.tree.statement.CompositeStatement;
import com.aliyun.fastmodel.core.tree.statement.constants.ConstraintType;
import com.aliyun.fastmodel.core.tree.statement.misc.Call;
import com.aliyun.fastmodel.core.tree.statement.select.order.NullOrdering;
import com.aliyun.fastmodel.core.tree.statement.select.order.OrderBy;
import com.aliyun.fastmodel.core.tree.statement.select.order.Ordering;
import com.aliyun.fastmodel.core.tree.statement.select.order.SortItem;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.PartitionedBy;
import com.aliyun.fastmodel.core.tree.statement.table.SetColComment;
import com.aliyun.fastmodel.core.tree.statement.table.SetTableComment;
import com.aliyun.fastmodel.core.tree.statement.table.SetTableProperties;
import com.aliyun.fastmodel.core.tree.statement.table.TableElement;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.BaseConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.DefaultValueConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.NotNullConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.PrimaryConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.UniqueConstraint;
import com.aliyun.fastmodel.core.tree.util.IdentifierUtil;
import com.aliyun.fastmodel.transform.api.context.ReverseContext;
import com.aliyun.fastmodel.transform.api.dialect.IVersion;
import com.aliyun.fastmodel.transform.hologres.client.property.HologresPropertyKey;
import com.aliyun.fastmodel.transform.hologres.dialect.HologresVersion;
import com.aliyun.fastmodel.transform.hologres.parser.HologreSQLParser.A_exprContext;
import com.aliyun.fastmodel.transform.hologres.parser.HologreSQLParser.A_expr_typecastContext;
import com.aliyun.fastmodel.transform.hologres.parser.HologreSQLParser.AexprconstContext;
import com.aliyun.fastmodel.transform.hologres.parser.HologreSQLParser.Any_nameContext;
import com.aliyun.fastmodel.transform.hologres.parser.HologreSQLParser.AnysconstContext;
import com.aliyun.fastmodel.transform.hologres.parser.HologreSQLParser.Array_boundsContext;
import com.aliyun.fastmodel.transform.hologres.parser.HologreSQLParser.BitwithlengthContext;
import com.aliyun.fastmodel.transform.hologres.parser.HologreSQLParser.BitwithoutlengthContext;
import com.aliyun.fastmodel.transform.hologres.parser.HologreSQLParser.Builtin_function_nameContext;
import com.aliyun.fastmodel.transform.hologres.parser.HologreSQLParser.C_expr_exprContext;
import com.aliyun.fastmodel.transform.hologres.parser.HologreSQLParser.CallstmtContext;
import com.aliyun.fastmodel.transform.hologres.parser.HologreSQLParser.CharacterContext;
import com.aliyun.fastmodel.transform.hologres.parser.HologreSQLParser.ColconstraintContext;
import com.aliyun.fastmodel.transform.hologres.parser.HologreSQLParser.ColconstraintelemContext;
import com.aliyun.fastmodel.transform.hologres.parser.HologreSQLParser.ColidContext;
import com.aliyun.fastmodel.transform.hologres.parser.HologreSQLParser.CollabelContext;
import com.aliyun.fastmodel.transform.hologres.parser.HologreSQLParser.ColumnDefContext;
import com.aliyun.fastmodel.transform.hologres.parser.HologreSQLParser.ColumnElemContext;
import com.aliyun.fastmodel.transform.hologres.parser.HologreSQLParser.ColumnlistContext;
import com.aliyun.fastmodel.transform.hologres.parser.HologreSQLParser.CommentColumnContext;
import com.aliyun.fastmodel.transform.hologres.parser.HologreSQLParser.CommentObjectTypeContext;
import com.aliyun.fastmodel.transform.hologres.parser.HologreSQLParser.Comment_textContext;
import com.aliyun.fastmodel.transform.hologres.parser.HologreSQLParser.ConstdatetimeContext;
import com.aliyun.fastmodel.transform.hologres.parser.HologreSQLParser.ConstraintelemContext;
import com.aliyun.fastmodel.transform.hologres.parser.HologreSQLParser.Create_dt_targetContext;
import com.aliyun.fastmodel.transform.hologres.parser.HologreSQLParser.CreateforeigntablestmtContext;
import com.aliyun.fastmodel.transform.hologres.parser.HologreSQLParser.CreatematviewstmtContext;
import com.aliyun.fastmodel.transform.hologres.parser.HologreSQLParser.CreatestmtContext;
import com.aliyun.fastmodel.transform.hologres.parser.HologreSQLParser.Dynamic_table_opt_column_elementContext;
import com.aliyun.fastmodel.transform.hologres.parser.HologreSQLParser.Dynamic_table_opt_column_listContext;
import com.aliyun.fastmodel.transform.hologres.parser.HologreSQLParser.FconstContext;
import com.aliyun.fastmodel.transform.hologres.parser.HologreSQLParser.Func_applicationContext;
import com.aliyun.fastmodel.transform.hologres.parser.HologreSQLParser.Func_arg_listContext;
import com.aliyun.fastmodel.transform.hologres.parser.HologreSQLParser.Func_exprContext;
import com.aliyun.fastmodel.transform.hologres.parser.HologreSQLParser.Func_nameContext;
import com.aliyun.fastmodel.transform.hologres.parser.HologreSQLParser.Generic_option_elemContext;
import com.aliyun.fastmodel.transform.hologres.parser.HologreSQLParser.GenerictypeContext;
import com.aliyun.fastmodel.transform.hologres.parser.HologreSQLParser.IconstContext;
import com.aliyun.fastmodel.transform.hologres.parser.HologreSQLParser.IndirectionContext;
import com.aliyun.fastmodel.transform.hologres.parser.HologreSQLParser.NameContext;
import com.aliyun.fastmodel.transform.hologres.parser.HologreSQLParser.NumericContext;
import com.aliyun.fastmodel.transform.hologres.parser.HologreSQLParser.Opt_array_boundsContext;
import com.aliyun.fastmodel.transform.hologres.parser.HologreSQLParser.Opt_dynamic_table_opt_column_listContext;
import com.aliyun.fastmodel.transform.hologres.parser.HologreSQLParser.Opt_sort_clauseContext;
import com.aliyun.fastmodel.transform.hologres.parser.HologreSQLParser.Opt_type_modifiersContext;
import com.aliyun.fastmodel.transform.hologres.parser.HologreSQLParser.OptpartitionspecContext;
import com.aliyun.fastmodel.transform.hologres.parser.HologreSQLParser.OpttableelementlistContext;
import com.aliyun.fastmodel.transform.hologres.parser.HologreSQLParser.Part_elemContext;
import com.aliyun.fastmodel.transform.hologres.parser.HologreSQLParser.Part_paramsContext;
import com.aliyun.fastmodel.transform.hologres.parser.HologreSQLParser.PartitionspecContext;
import com.aliyun.fastmodel.transform.hologres.parser.HologreSQLParser.Qualified_nameContext;
import com.aliyun.fastmodel.transform.hologres.parser.HologreSQLParser.Reloption_elemContext;
import com.aliyun.fastmodel.transform.hologres.parser.HologreSQLParser.SconstContext;
import com.aliyun.fastmodel.transform.hologres.parser.HologreSQLParser.SimpletypenameContext;
import com.aliyun.fastmodel.transform.hologres.parser.HologreSQLParser.SinglestmtContext;
import com.aliyun.fastmodel.transform.hologres.parser.HologreSQLParser.Sort_clauseContext;
import com.aliyun.fastmodel.transform.hologres.parser.HologreSQLParser.SortbyContext;
import com.aliyun.fastmodel.transform.hologres.parser.HologreSQLParser.StmtmultiContext;
import com.aliyun.fastmodel.transform.hologres.parser.HologreSQLParser.TableconstraintContext;
import com.aliyun.fastmodel.transform.hologres.parser.HologreSQLParser.TransactionstmtContext;
import com.aliyun.fastmodel.transform.hologres.parser.HologreSQLParser.Type_function_nameContext;
import com.aliyun.fastmodel.transform.hologres.parser.HologreSQLParser.TypenameContext;
import com.aliyun.fastmodel.transform.hologres.parser.tree.BeginWork;
import com.aliyun.fastmodel.transform.hologres.parser.tree.CommitWork;
import com.aliyun.fastmodel.transform.hologres.parser.tree.datatype.ArrayBounds;
import com.aliyun.fastmodel.transform.hologres.parser.tree.datatype.HologresArrayDataType;
import com.aliyun.fastmodel.transform.hologres.parser.tree.datatype.HologresDataTypeName;
import com.aliyun.fastmodel.transform.hologres.parser.tree.datatype.HologresGenericDataType;
import com.aliyun.fastmodel.transform.hologres.parser.tree.datatype.HologresRowDataType;
import com.aliyun.fastmodel.transform.hologres.parser.tree.datatype.HologresRowDataType.RowType;
import com.aliyun.fastmodel.transform.hologres.parser.tree.expr.WithDataTypeNameExpression;
import com.aliyun.fastmodel.transform.hologres.parser.util.HologresPropertyUtil;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.Getter;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * HologresAstBuilder
 *
 * @author panguanjing
 * @date 2022/6/7
 */
@Getter
public class HologresAstBuilder extends HologreSQLParserBaseVisitor<Node> {

    /**
     * set table property
     */
    public static final String SET_TABLE_PROPERTY = "set_table_property";
    /**
     * set table property arg size
     */
    public static final int SET_TABLE_PROPERTY_ARG_SIZE = 3;

    private final ReverseContext context;

    public HologresAstBuilder(ReverseContext context) {
        this.context = context == null ? ReverseContext.builder().build() : context;
    }

    private HologresVersion getVersion(ReverseContext context) {
        IVersion version = context.getVersion();
        if (version == null) {
            return HologresVersion.V1;
        }
        return HologresVersion.getByValue(version.getName());
    }

    @Override
    public Node visitRoot(HologreSQLParser.RootContext ctx) {
        return visit(ctx.stmtblock());
    }

    @Override
    public Node visitStmtmulti(StmtmultiContext ctx) {
        List<BaseStatement> visit = ParserHelper.visit(this, ctx.singlestmt(), BaseStatement.class);
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
    public Node visitSinglestmt(SinglestmtContext ctx) {
        return visit(ctx.stmt());
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
                    return new HologresArrayDataType(baseDataType, list);
                }
                return baseDataType;
            } else if (ctx.ARRAY() != null) {
                if (ctx.iconst() == null) {
                    return new HologresArrayDataType(baseDataType, null);
                } else {
                    LongLiteral longLiteral = (LongLiteral)visit(ctx.iconst());
                    ArrayBounds arrayBounds = new ArrayBounds(longLiteral.getValue().intValue());
                    return new HologresArrayDataType(baseDataType, Lists.newArrayList(arrayBounds));
                }
            } else {
                return baseDataType;
            }
        }
        if (ctx.qualified_name() != null) {
            QualifiedName qualifiedName = (QualifiedName)visit(ctx.qualified_name());
            if (ctx.ROWTYPE() != null) {
                return new HologresRowDataType(qualifiedName, RowType.ROWTYPE);
            }
            if (ctx.TYPE_P() != null) {
                return new HologresRowDataType(qualifiedName, RowType.TYPE);
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
            columnDefinitions = visit.stream().filter(
                t -> t instanceof ColumnDefinition
            ).map(t -> {
                return (ColumnDefinition)t;
            }).collect(Collectors.toList());

            constraints = visit.stream().filter(
                t -> t instanceof BaseConstraint
            ).map(t -> {
                return (BaseConstraint)t;
            }).collect(Collectors.toList());
        }
        PartitionedBy partitionedBy = null;
        if (ctx.optpartitionspec() != null && ctx.optpartitionspec().getChildCount() > 0 && columnDefinitions != null) {
            partitionedBy = (PartitionedBy)visit(ctx.optpartitionspec());
            partitionedBy = mapColumn(partitionedBy, columnDefinitions);
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
    public Node visitCreatematviewstmt(CreatematviewstmtContext ctx) {
        if (ctx.DYNAMIC() == null) {
            return null;
        }
        //CREATE opttemp DYNAMIC TABLE (IF_P NOT EXISTS)? create_dt_target AS selectstmt opt_with_data
        // CREATE opttemp DYNAMIC TABLE (IF_P NOT EXISTS)? create_dt_target2
        //if not exists
        boolean ifNotExist = ctx.IF_P() != null;
        Create_dt_targetContext target = ctx.create_dt_target();
        Qualified_nameContext qualified_nameContext = target.qualified_name();
        QualifiedName tableName = (QualifiedName)visit(qualified_nameContext);
        //column
        Opt_dynamic_table_opt_column_listContext opt_dynamic_table_opt_column_listContext = target.opt_dynamic_table_opt_column_list();
        List<ColumnDefinition> list = null;
        if (opt_dynamic_table_opt_column_listContext != null) {
            list = toColumnDef(opt_dynamic_table_opt_column_listContext.dynamic_table_opt_column_list());
        }

        // paritition by
        OptpartitionspecContext optpartitionspec = target.optpartitionspec();
        PartitionedBy partitionedBy = toPartitionBy(optpartitionspec);

        //dynamic table
        List<Property> propertyList = Lists.newArrayList();
        propertyList.add(new Property(HologresPropertyKey.DYNAMIC.getValue(), "true"));

        //property
        if (target.opt_reloptions() != null) {
            List<Property> other = ParserHelper.visit(this, target.opt_reloptions().reloptions().reloption_list().reloption_elem(),
                Property.class);
            if (other != null) {
                propertyList.addAll(other);
            }
        }

        //return
        if (ctx.selectstmt() != null) {
            String origin = ParserHelper.getOrigin(ctx.selectstmt());
            Property property = new Property(HologresPropertyKey.TASK_DEFINITION.getValue(), origin);
            propertyList.add(property);
        }
        return CreateTable.builder()
            .ifNotExist(ifNotExist)
            .tableName(tableName)
            .columns(list)
            .partition(partitionedBy)
            .properties(propertyList)
            .build();
    }

    /**
     * 分区信息
     *
     * @param optpartitionspec
     * @return
     */
    private PartitionedBy toPartitionBy(OptpartitionspecContext optpartitionspec) {
        if (optpartitionspec == null) {
            return null;
        }
        PartitionspecContext partitionspec = optpartitionspec.partitionspec();
        if (partitionspec == null) {
            return null;
        }
        Part_paramsContext part_paramsContext = partitionspec.part_params();
        if (part_paramsContext == null) {
            return null;
        }
        List<Part_elemContext> part_elemContexts = part_paramsContext.part_elem();
        List<Identifier> identifiers = ParserHelper.visit(this, part_elemContexts, Identifier.class);
        List<ColumnDefinition> columnDefinitions = identifiers.stream().map(c ->
            ColumnDefinition.builder().colName(c).build()
        ).collect(Collectors.toList());
        return new PartitionedBy(columnDefinitions);
    }

    /**
     * 列信息
     * dynamic_table_opt_column_list
     * :   dynamic_table_opt_column_element
     * |  dynamic_table_opt_column_list COMMA dynamic_table_opt_column_element
     * ;
     *
     * @param columnListContext
     * @return
     */
    private List<ColumnDefinition> toColumnDef(Dynamic_table_opt_column_listContext columnListContext) {
        if (columnListContext == null) {
            return null;
        }
        int childCount = columnListContext.getChildCount();
        List<ColumnDefinition> all = Lists.newArrayList();
        for (int i = 0; i < childCount; i++) {
            ParseTree child = columnListContext.getChild(i);
            if (child instanceof Dynamic_table_opt_column_listContext) {
                List<ColumnDefinition> t = toColumnDef((Dynamic_table_opt_column_listContext)child);
                all.addAll(t);
            } else if (child instanceof Dynamic_table_opt_column_elementContext) {
                ColumnDefinition columnDefinition = (ColumnDefinition)visit(child);
                all.add(columnDefinition);
            }
        }
        return all;
    }

    @Override
    public Node visitDynamic_table_opt_column_element(Dynamic_table_opt_column_elementContext ctx) {
        Identifier identifier = (Identifier)visit(ctx.columnElem());
        return ColumnDefinition.builder()
            .colName(identifier)
            .build();
    }

    @Override
    public Node visitCreateforeigntablestmt(HologreSQLParser.CreateforeigntablestmtContext ctx) {
        QualifiedName qualifiedName = (QualifiedName)visit(ctx.qualified_name(0));
        boolean isNotExist = ctx.EXISTS() != null && ctx.NOT() != null;
        OpttableelementlistContext opttableelementlist = ctx.opttableelementlist();
        List<ColumnDefinition> columnDefinitions = null;
        List<BaseConstraint> constraints = null;
        if (opttableelementlist != null) {
            List<TableElement> visit = ParserHelper.visit(this, opttableelementlist.tableelementlist().tableelement(), TableElement.class);
            columnDefinitions = visit.stream().filter(
                t -> t instanceof ColumnDefinition
            ).map(t -> (ColumnDefinition)t).collect(Collectors.toList());

            constraints = visit.stream().filter(
                t -> t instanceof BaseConstraint
            ).map(t -> (BaseConstraint)t).collect(Collectors.toList());
        }

        List<Property> properties = new ArrayList<>();
        if (ctx.FOREIGN() != null) {
            properties.add(new Property(HologresPropertyKey.FOREIGN.getValue(), new BooleanLiteral(BooleanLiteral.TRUE)));
        }

        // server
        if (ctx.SERVER() != null) {
            Identifier serverName = (Identifier)visit(ctx.name().colid());
            properties.add(new Property(HologresPropertyKey.SERVER_NAME.getValue(), serverName.getValue()));
        }

        //options
        List<Property> options = buildOptions(ctx);
        properties.addAll(options);

        return CreateTable.builder()
            .ifNotExist(isNotExist)
            .columns(columnDefinitions)
            .tableName(qualifiedName)
            .constraints(constraints)
            .properties(properties)
            .build();
    }

    @Override
    public Property visitReloption_elem(HologreSQLParser.Reloption_elemContext ctx) {
        String key = ctx.collabel().get(0).identifier().Identifier().getText();
        String value = ctx.def_arg().sconst().anysconst().StringConstant().getText();
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
        return new PartitionedBy(
            list.stream().map(c -> ColumnDefinition.builder()
                .colName(c)
                .build()).collect(Collectors.toList())
        );
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
        return ColumnDefinition.builder()
            .colName(identifier)
            .dataType(baseDataType)
            .notNull(toNotNull(inlineConstraint))
            .primary(toPrimary(inlineConstraint))
            .defaultValue(toDefaultValue(inlineConstraint))
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
        return super.visitColconstraintelem(ctx);
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
    public Node visitCallstmt(CallstmtContext ctx) {
        FunctionCall functionCall = (FunctionCall)visit(ctx.func_application());
        QualifiedName funcName = functionCall.getFuncName();
        Call call = new Call(functionCall);
        if (!StringUtils.equalsIgnoreCase(funcName.getSuffix(), SET_TABLE_PROPERTY)) {
            return call;
        }
        List<BaseExpression> arguments = functionCall.getArguments();
        if (arguments.size() != SET_TABLE_PROPERTY_ARG_SIZE) {
            return call;
        }
        BaseExpression baseExpression = arguments.get(0);
        if (!(baseExpression instanceof StringLiteral)) {
            return call;
        }
        StringLiteral tableOrColumn = (StringLiteral)baseExpression;
        StringLiteral propertyKey = (StringLiteral)arguments.get(1);
        StringLiteral propertyValue = (StringLiteral)arguments.get(2);
        Property property = new Property(
            propertyKey.getValue(),
            convertUnionValue(propertyKey.getValue(), propertyValue.getValue())
        );
        return new SetTableProperties(
            QualifiedName.of(tableOrColumn.getValue()),
            Collections.singletonList(property)
        );
    }

    /**
     * 根据key，将值转为统一的value
     *
     * @param key
     * @param value
     * @return
     */
    private String convertUnionValue(String key, String value) {
        HologresPropertyKey byValue = HologresPropertyKey.getByValue(key);
        //如果不在我们指定的内容，那么直接返回
        if (byValue == null) {
            return value;
        }
        return HologresPropertyUtil.getPropertyValue(getVersion(context), key, value);
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
        FunctionCall functionCall = new FunctionCall(
            functionName,
            ctx.DISTINCT() != null,
            list,
            null,
            null,
            null,
            orderBy
        );
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
    public Node visitB_expr(HologreSQLParser.B_exprContext ctx) {
        if (ctx.c_expr() != null && ctx.c_expr() instanceof C_expr_exprContext) {
            C_expr_exprContext exprExprContext = (C_expr_exprContext)ctx.c_expr();
            if (exprExprContext.func_expr() != null) {
                return visit(exprExprContext.func_expr());
            }
        }
        if (ctx.typename() != null) {
            BaseExpression baseExpression = (BaseExpression)visit(ctx.b_expr(0));
            BaseDataType dataTypeName = (BaseDataType)visit(ctx.typename());
            return new WithDataTypeNameExpression(ParserHelper.getLocation(ctx), ParserHelper.getOrigin(ctx),
                baseExpression, dataTypeName
            );
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
        return new OrderBy(
            list
        );
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
        SortItem sortItem = new SortItem(
            baseExpression,
            ordering,
            nullOrdering
        );
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
            String name = HologresDataTypeName.INTERVAL.getValue();
            if (ctx.OPEN_PAREN() != null) {
                List<DataTypeParameter> list = Lists.newArrayList();
                LongLiteral visit = (LongLiteral)visit(ctx.iconst());
                DataTypeParameter dataTypeParameter = new NumericParameter(visit.getValue().toString());
                list.add(dataTypeParameter);
                return new HologresGenericDataType(name, list);
            } else {
                return new HologresGenericDataType(name);
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
        String text = ParserHelper.getOrigin(ctx.type_function_name());
        return new HologresGenericDataType(
            text,
            list
        );
    }

    @Override
    public Node visitNumeric(NumericContext ctx) {
        String text = ctx.dataTypeName.getText();
        if (StringUtils.equalsIgnoreCase(text, DataTypeEnums.DOUBLE.getName())) {
            return new HologresGenericDataType(HologresDataTypeName.DOUBLE_PRECISION.getValue());
        }
        if (StringUtils.equalsIgnoreCase(text, DataTypeEnums.DECIMAL.name()) || StringUtils.equalsIgnoreCase(text,
            HologresDataTypeName.DECIMAL.getAlias())) {
            Opt_type_modifiersContext opt_type_modifiersContext = ctx.opt_type_modifiers();
            if (opt_type_modifiersContext != null && opt_type_modifiersContext.expr_list() != null) {
                List<A_exprContext> aExprContexts = opt_type_modifiersContext.expr_list().a_expr();
                List<DataTypeParameter> list = getDataTypeParameters(aExprContexts);
                return new HologresGenericDataType(HologresDataTypeName.DECIMAL.getValue(), list);
            } else {
                return new HologresGenericDataType(HologresDataTypeName.DECIMAL.getValue());
            }
        }
        return new HologresGenericDataType(text);
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
    public Node visitBitwithlength(BitwithlengthContext ctx) {
        return new HologresGenericDataType(
            ctx.BIT().getText(),
            getDataTypeParameters(ctx.expr_list().a_expr())
        );
    }

    @Override
    public Node visitBitwithoutlength(BitwithoutlengthContext ctx) {
        return new HologresGenericDataType(ctx.BIT().getText());
    }

    @Override
    public Node visitCharacter(CharacterContext ctx) {
        DataTypeParameter typeParameter = null;
        if (ctx.iconst() != null) {
            LongLiteral longLiteral = (LongLiteral)visit(ctx.iconst());
            typeParameter = new NumericParameter(longLiteral.getValue().toString());
        }
        return new HologresGenericDataType(
            ParserHelper.getOrigin(ctx.character_c()),
            typeParameter == null ? Collections.emptyList() : Lists.newArrayList(typeParameter)
        );
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
            return new HologresGenericDataType(name,
                arguments
            );
        }
        boolean without = ctx.opt_timezone().WITHOUT() != null;
        if (ctx.TIME() != null) {
            HologresDataTypeName dataTypeName = without ? HologresDataTypeName.TIME : HologresDataTypeName.TIMETZ;
            return new HologresGenericDataType(dataTypeName.getValue(), arguments);
        }
        if (ctx.TIMESTAMP() != null) {
            HologresDataTypeName dataTypeName = without ? HologresDataTypeName.TIMESTAMP : HologresDataTypeName.TIMESTAMPTZ;
            return new HologresGenericDataType(dataTypeName.getValue(), arguments);
        }
        throw new UnsupportedOperationException("unsupported dataType of " + name);
    }

    @Override
    public Node visitCommentColumn(CommentColumnContext ctx) {
        QualifiedName qualifiedName = (QualifiedName)visit(ctx.any_name());
        Comment comment = (Comment)visit(ctx.comment_text());
        List<Identifier> originalParts = qualifiedName.getOriginalParts();
        QualifiedName tableName = QualifiedName.of(originalParts.subList(0, originalParts.size() - 1));
        return new SetColComment(
            tableName,
            originalParts.get(originalParts.size() - 1),
            comment
        );
    }

    @Override
    public Node visitCommentObjectType(CommentObjectTypeContext ctx) {
        QualifiedName qualifiedName = (QualifiedName)visit(ctx.any_name());
        Comment comment = (Comment)visit(ctx.comment_text());
        return new SetTableComment(qualifiedName, comment);
    }

    @Override
    public Node visitComment_text(Comment_textContext ctx) {
        if (ctx.NULL_P() != null) {
            return new Comment(null);
        }
        return new Comment(StripUtils.strip(ctx.getText()));
    }

    @Override
    public Node visitGeneric_option_elem(HologreSQLParser.Generic_option_elemContext ctx) {
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
        List<Generic_option_elemContext> genericOptionElemContexts =
            ctx.create_generic_options().generic_option_list().generic_option_elem();
        genericOptionElemContexts.forEach(genericOptionElemContext -> {
            Property property = (Property)visit(genericOptionElemContext);
            options.add(property);
        });
        return options;
    }

    /**
     * func_application
     * : func_name OPEN_PAREN (
     * func_arg_list (COMMA VARIADIC func_arg_expr)? opt_sort_clause
     * | VARIADIC func_arg_expr opt_sort_clause
     * | (ALL | DISTINCT) func_arg_list opt_sort_clause
     * | STAR
     * |
     * ) CLOSE_PAREN
     * ;
     *
     * @param ctx the parse tree
     * @return Node
     */
    @Override
    public Node visitFunc_expr(Func_exprContext ctx) {
        Func_applicationContext funcApplicationContext = ctx.func_application();
        if (funcApplicationContext == null) {
            return null;
        }
        List<BaseExpression> argument = Lists.newArrayList();
        if (funcApplicationContext.STAR() != null) {
            return new FunctionCall(
                (QualifiedName)visit(funcApplicationContext.func_name()),
                funcApplicationContext.DISTINCT() != null,
                argument
            );
        }
        if (funcApplicationContext.func_arg_list() != null) {
            argument = ParserHelper.visit(this, funcApplicationContext.func_arg_list().func_arg_expr(), BaseExpression.class);
        }
        return new FunctionCall(
            (QualifiedName)visit(funcApplicationContext.func_name()),
            funcApplicationContext.DISTINCT() != null,
            argument
        );
    }
}
