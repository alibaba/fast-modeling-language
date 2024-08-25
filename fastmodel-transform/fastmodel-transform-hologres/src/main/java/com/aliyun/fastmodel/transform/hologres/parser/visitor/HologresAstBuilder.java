/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.hologres.parser.visitor;

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
import com.aliyun.fastmodel.transform.hologres.client.property.HoloPropertyKey;
import com.aliyun.fastmodel.transform.hologres.dialect.HologresVersion;
import com.aliyun.fastmodel.transform.hologres.parser.PostgreSQLParser;
import com.aliyun.fastmodel.transform.hologres.parser.PostgreSQLParser.A_exprContext;
import com.aliyun.fastmodel.transform.hologres.parser.PostgreSQLParser.AexprconstContext;
import com.aliyun.fastmodel.transform.hologres.parser.PostgreSQLParser.Any_nameContext;
import com.aliyun.fastmodel.transform.hologres.parser.PostgreSQLParser.AnysconstContext;
import com.aliyun.fastmodel.transform.hologres.parser.PostgreSQLParser.Array_boundsContext;
import com.aliyun.fastmodel.transform.hologres.parser.PostgreSQLParser.BitwithlengthContext;
import com.aliyun.fastmodel.transform.hologres.parser.PostgreSQLParser.BitwithoutlengthContext;
import com.aliyun.fastmodel.transform.hologres.parser.PostgreSQLParser.C_expr_exprContext;
import com.aliyun.fastmodel.transform.hologres.parser.PostgreSQLParser.CallstmtContext;
import com.aliyun.fastmodel.transform.hologres.parser.PostgreSQLParser.CharacterContext;
import com.aliyun.fastmodel.transform.hologres.parser.PostgreSQLParser.ColconstraintContext;
import com.aliyun.fastmodel.transform.hologres.parser.PostgreSQLParser.ColconstraintelemContext;
import com.aliyun.fastmodel.transform.hologres.parser.PostgreSQLParser.ColidContext;
import com.aliyun.fastmodel.transform.hologres.parser.PostgreSQLParser.CollabelContext;
import com.aliyun.fastmodel.transform.hologres.parser.PostgreSQLParser.ColumnDefContext;
import com.aliyun.fastmodel.transform.hologres.parser.PostgreSQLParser.ColumnElemContext;
import com.aliyun.fastmodel.transform.hologres.parser.PostgreSQLParser.ColumnlistContext;
import com.aliyun.fastmodel.transform.hologres.parser.PostgreSQLParser.CommentColumnContext;
import com.aliyun.fastmodel.transform.hologres.parser.PostgreSQLParser.CommentObjectTypeContext;
import com.aliyun.fastmodel.transform.hologres.parser.PostgreSQLParser.Comment_textContext;
import com.aliyun.fastmodel.transform.hologres.parser.PostgreSQLParser.ConstdatetimeContext;
import com.aliyun.fastmodel.transform.hologres.parser.PostgreSQLParser.ConstraintelemContext;
import com.aliyun.fastmodel.transform.hologres.parser.PostgreSQLParser.CreateforeigntablestmtContext;
import com.aliyun.fastmodel.transform.hologres.parser.PostgreSQLParser.CreatestmtContext;
import com.aliyun.fastmodel.transform.hologres.parser.PostgreSQLParser.FconstContext;
import com.aliyun.fastmodel.transform.hologres.parser.PostgreSQLParser.Func_applicationContext;
import com.aliyun.fastmodel.transform.hologres.parser.PostgreSQLParser.Func_arg_listContext;
import com.aliyun.fastmodel.transform.hologres.parser.PostgreSQLParser.Func_nameContext;
import com.aliyun.fastmodel.transform.hologres.parser.PostgreSQLParser.Generic_option_elemContext;
import com.aliyun.fastmodel.transform.hologres.parser.PostgreSQLParser.GenerictypeContext;
import com.aliyun.fastmodel.transform.hologres.parser.PostgreSQLParser.IconstContext;
import com.aliyun.fastmodel.transform.hologres.parser.PostgreSQLParser.IndirectionContext;
import com.aliyun.fastmodel.transform.hologres.parser.PostgreSQLParser.NameContext;
import com.aliyun.fastmodel.transform.hologres.parser.PostgreSQLParser.NumericContext;
import com.aliyun.fastmodel.transform.hologres.parser.PostgreSQLParser.Opt_array_boundsContext;
import com.aliyun.fastmodel.transform.hologres.parser.PostgreSQLParser.Opt_sort_clauseContext;
import com.aliyun.fastmodel.transform.hologres.parser.PostgreSQLParser.Opt_type_modifiersContext;
import com.aliyun.fastmodel.transform.hologres.parser.PostgreSQLParser.OptpartitionspecContext;
import com.aliyun.fastmodel.transform.hologres.parser.PostgreSQLParser.OpttableelementlistContext;
import com.aliyun.fastmodel.transform.hologres.parser.PostgreSQLParser.Part_elemContext;
import com.aliyun.fastmodel.transform.hologres.parser.PostgreSQLParser.PartitionspecContext;
import com.aliyun.fastmodel.transform.hologres.parser.PostgreSQLParser.Qualified_nameContext;
import com.aliyun.fastmodel.transform.hologres.parser.PostgreSQLParser.Reloption_elemContext;
import com.aliyun.fastmodel.transform.hologres.parser.PostgreSQLParser.SconstContext;
import com.aliyun.fastmodel.transform.hologres.parser.PostgreSQLParser.SimpletypenameContext;
import com.aliyun.fastmodel.transform.hologres.parser.PostgreSQLParser.Sort_clauseContext;
import com.aliyun.fastmodel.transform.hologres.parser.PostgreSQLParser.SortbyContext;
import com.aliyun.fastmodel.transform.hologres.parser.PostgreSQLParser.StmtmultiContext;
import com.aliyun.fastmodel.transform.hologres.parser.PostgreSQLParser.TableconstraintContext;
import com.aliyun.fastmodel.transform.hologres.parser.PostgreSQLParser.TransactionstmtContext;
import com.aliyun.fastmodel.transform.hologres.parser.PostgreSQLParser.Type_function_nameContext;
import com.aliyun.fastmodel.transform.hologres.parser.PostgreSQLParser.TypenameContext;
import com.aliyun.fastmodel.transform.hologres.parser.PostgreSQLParserBaseVisitor;
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
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * HologresAstBuilder
 *
 * @author panguanjing
 * @date 2022/6/7
 */
@Getter
public class HologresAstBuilder extends PostgreSQLParserBaseVisitor<Node> {

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
    public Node visitRoot(PostgreSQLParser.RootContext ctx) {
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
    public Node visitCreateforeigntablestmt(PostgreSQLParser.CreateforeigntablestmtContext ctx) {
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
            properties.add(new Property(HoloPropertyKey.FOREIGN.getValue(), new BooleanLiteral(BooleanLiteral.TRUE)));
        }

        // server
        if (ctx.SERVER() != null) {
            Identifier serverName = (Identifier)visit(ctx.name().colid());
            properties.add(new Property(HoloPropertyKey.SERVER_NAME.getValue(), serverName.getValue()));
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
    public Property visitReloption_elem(PostgreSQLParser.Reloption_elemContext ctx) {
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
        HoloPropertyKey byValue = HoloPropertyKey.getByValue(key);
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
    public Node visitB_expr(PostgreSQLParser.B_exprContext ctx) {
        if (ctx.c_expr() != null && ctx.c_expr() instanceof C_expr_exprContext) {
            C_expr_exprContext exprExprContext = (C_expr_exprContext)ctx.c_expr();
            if (exprExprContext.func_expr() != null && exprExprContext.func_expr().func_application() != null) {
                return visit(exprExprContext.func_expr().func_application());
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
        } else {
            Identifier identifier = (Identifier)visit(ctx.type_function_name());
            return QualifiedName.of(Lists.newArrayList(identifier));
        }
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
        String text = ctx.type_function_name().getText();
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
        if (StringUtils.equalsIgnoreCase(text, DataTypeEnums.DECIMAL.name())) {
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
            ctx.character_c().getText(),
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
        if (without) {
            throw new UnsupportedOperationException("unsupported without zone dataType");
        } else {
            return new HologresGenericDataType(
                HologresDataTypeName.TIMESTAMPTZ.getValue(),
                arguments
            );
        }
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
    public Node visitGeneric_option_elem(PostgreSQLParser.Generic_option_elemContext ctx) {
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

}
