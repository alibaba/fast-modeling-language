/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.api.client.converter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeParameter;
import com.aliyun.fastmodel.core.tree.datatype.GenericDataType;
import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName;
import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName.Dimension;
import com.aliyun.fastmodel.core.tree.datatype.NumericParameter;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.constants.ColumnPropertyDefaultKey;
import com.aliyun.fastmodel.core.tree.statement.constants.ConstraintScope;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.PartitionedBy;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.BaseConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.PrimaryConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.UniqueConstraint;
import com.aliyun.fastmodel.core.tree.util.IdentifierUtil;
import com.aliyun.fastmodel.transform.api.client.ClientConverter;
import com.aliyun.fastmodel.transform.api.client.PropertyConverter;
import com.aliyun.fastmodel.transform.api.client.dto.constraint.Constraint;
import com.aliyun.fastmodel.transform.api.client.dto.constraint.ConstraintType;
import com.aliyun.fastmodel.transform.api.client.dto.constraint.OutlineConstraintType;
import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.aliyun.fastmodel.transform.api.client.dto.table.Column;
import com.aliyun.fastmodel.transform.api.client.dto.table.Table;
import com.aliyun.fastmodel.transform.api.client.dto.table.TableConfig;
import com.aliyun.fastmodel.transform.api.context.TransformContext;
import com.aliyun.fastmodel.transform.api.util.StringJoinUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * basic client converter
 *
 * @author panguanjing
 * @date 2022/8/5
 */
public abstract class BaseClientConverter<T extends TransformContext> implements ClientConverter<T> {

    public static final String ONE_DIMENSION = "%s(%d)";
    public static final String TWO_DIMENSION = "%s(%d,%d)";
    public static final int SECOND_INDEX = 2;
    public static final int FIRST_INDEX = 1;
    public static final int THIRD_INDEX = 3;

    /**
     * @param table
     * @return {@link Node}
     */
    @Override
    public Node covertToNode(Table table, TableConfig tableConfig) {
        QualifiedName of = StringJoinUtil.join(table.getDatabase(), table.getSchema(), table.getName());
        Comment comment = null;
        if (table.getComment() != null) {
            comment = new Comment(table.getComment());
        }
        List<BaseConstraint> constraints = toConstraint(table.getColumns(), table.getConstraints());
        PartitionedBy partitionedBy = toPartitionedBy(table, table.getColumns());
        List<Property> properties = toProperty(table, table.getProperties());
        List<ColumnDefinition> columnDefines = toColumnDefinition(table, table.getColumns());
        return CreateTable.builder()
            .ifNotExist(table.isIfNotExist())
            .tableName(of)
            .columns(columnDefines)
            .partition(partitionedBy)
            .comment(comment)
            .constraints(constraints)
            .properties(properties)
            .build();
    }

    /**
     * 将node转为Table
     *
     * @param table
     * @param context
     * @return
     */
    @Override
    public Table convertToTable(Node table, T context) {
        Preconditions.checkArgument(table instanceof CreateTable, "unsupported convert to table:" + table.getClass());
        CreateTable createTable = (CreateTable)table;
        String schema = toSchema(createTable, context);
        String suffix = createTable.getQualifiedName().getSuffix();
        String database = toDatabase(createTable, context.getDatabase());
        List<Column> columns = toTableColumns(createTable);
        List<Constraint> constraints = toOutlineConstraint(createTable);
        List<BaseClientProperty> properties = toBaseClientProperty(createTable);
        return Table.builder()
            .ifNotExist(createTable.isNotExists())
            .external(isExternal(createTable))
            .database(database)
            .schema(schema).name(suffix)
            .comment(createTable.getCommentValue())
            .lifecycleSeconds(toLifeCycleSeconds(createTable))
            .columns(columns)
            .constraints(constraints)
            .properties(properties)
            .build();
    }



    protected String toSchema(CreateTable createTable, T transformContext) {
        QualifiedName qualifiedName = createTable.getQualifiedName();
        if (!qualifiedName.isJoinPath()) {
            return transformContext.getSchema();
        }
        boolean isSecondSchema = qualifiedName.getOriginalParts().size() == SECOND_INDEX;
        if (isSecondSchema) {
            return qualifiedName.getFirst();
        }
        boolean isThirdSchema = qualifiedName.getOriginalParts().size() == THIRD_INDEX;
        if (isThirdSchema) {
            return qualifiedName.getParts().get(1);
        }
        return transformContext.getSchema();
    }

    protected String toDatabase(CreateTable createTable, String database) {
        QualifiedName qualifiedName = createTable.getQualifiedName();
        boolean isThirdSchema = qualifiedName.isJoinPath() && qualifiedName.getOriginalParts().size() == THIRD_INDEX;
        if (isThirdSchema) {
            return qualifiedName.getFirst();
        }
        return database;
    }

    protected List<BaseClientProperty> toBaseClientProperty(CreateTable createTable) {
        List<Property> properties = createTable.getProperties();
        if (createTable.isPropertyEmpty()) {
            return Lists.newArrayList();
        }
        List<BaseClientProperty> baseClientProperties = Lists.newArrayList();
        for (Property property : properties) {
            String name = property.getName();
            String value = property.getValue();
            BaseClientProperty baseClientProperty = getPropertyConverter().create(name, value);
            if (baseClientProperty == null) {
                continue;
            }
            baseClientProperties.add(baseClientProperty);
        }
        return baseClientProperties;
    }

    /**
     * get property converter
     *
     * @return {@link PropertyConverter}
     */
    public abstract PropertyConverter getPropertyConverter();

    /**
     * to lifecycle
     *
     * @param createTable
     * @return
     */
    protected Long toLifeCycleSeconds(CreateTable createTable) {
        return 0L;
    }

    /**
     * is external
     *
     * @param createTable
     * @return
     */
    protected Boolean isExternal(CreateTable createTable) {
        return false;
    }

    /**
     * toTableColumns
     *
     * @param createTable
     * @return
     */
    public List<Column> toTableColumns(CreateTable createTable) {
        List<Column> list = Lists.newArrayList();
        List<ColumnDefinition> columnDefines = createTable.getColumnDefines();
        List<BaseConstraint> constraints = createTable.getConstraintStatements();
        if (!createTable.isColumnEmpty()) {
            List<Column> columns = columnDefines.stream().map(c -> {
                Column column = getColumn(c, false, null);
                setPrimaryKey(constraints, column);
                return column;
            }).collect(Collectors.toList());
            list.addAll(columns);
        }
        if (createTable.isPartitionEmpty()) {return list;}
        //分区信息，如果column里面已经含有分区信息，那么更新下
        List<ColumnDefinition> partitionColumns = createTable.getPartitionedBy().getColumnDefinitions();
        int index = 0;
        //原有的列中是否含有分区列，如果含有，那么设置下分区信息
        for (Column c : list) {
            if (contains(partitionColumns, c)) {
                Integer partitionKeyIndex = getPartitionKeyIndex(partitionColumns, c);
                c.setPartitionKey(true);
                c.setPartitionKeyIndex(partitionKeyIndex);
                index++;
            }
        }
        //如果list不包含分区列，那么将分区列加入到原有的列中
        int start = index;
        for (ColumnDefinition columnDefinition : partitionColumns) {
            if (!contains(list, columnDefinition)) {
                Column column = getColumn(columnDefinition, true, start++);
                setPrimaryKey(constraints, column);
                list.add(column);
            }
        }
        return list;
    }

    private Integer getPartitionKeyIndex(List<ColumnDefinition> partitionColumns, Column c) {
        int i = -1;
        OptionalInt first = IntStream.range(0, partitionColumns.size()).filter(
            index -> {
                ColumnDefinition columnDefinition = partitionColumns.get(index);
                return (Objects.equals(new Identifier(c.getName()), columnDefinition.getColName()));
            }
        ).findFirst();
        return first.isPresent() ? first.getAsInt() : -1;
    }

    /**
     * to outline constraint
     *
     * @param createTable
     * @return
     */
    protected List<Constraint> toOutlineConstraint(CreateTable createTable) {
        List<BaseConstraint> constraintStatements = createTable.getConstraintStatements();
        if (CollectionUtils.isEmpty(constraintStatements)) {
            return Collections.emptyList();
        }
        List<Constraint> list = Lists.newArrayList();
        //outline constraint only primary key and unique key
        for (BaseConstraint constraint : constraintStatements) {
            Constraint constraintDto = new Constraint();
            constraintDto.setName(constraintDto.getName());
            ConstraintScope scope = constraint.getConstraintType().getScope();
            constraintDto.setType(OutlineConstraintType.getByValue(constraint.getConstraintType().getCode()));
            if (scope != ConstraintScope.COLUMN) {
                continue;
            }
            if (constraint instanceof PrimaryConstraint) {
                PrimaryConstraint primaryConstraint = (PrimaryConstraint)constraint;
                List<String> columns = primaryConstraint.getColNames().stream().map(Identifier::getValue).collect(Collectors.toList());
                constraintDto.setColumns(columns);
            }
            if (constraint instanceof UniqueConstraint) {
                UniqueConstraint primaryConstraint = (UniqueConstraint)constraint;
                List<String> columns = primaryConstraint.getColumnNames().stream().map(Identifier::getValue).collect(Collectors.toList());
                constraintDto.setColumns(columns);
            }
            list.add(constraintDto);
        }
        return list;
    }

    /**
     * to column definition
     *
     * @param table
     * @param columns
     * @return
     */
    protected List<ColumnDefinition> toColumnDefinition(Table table, List<Column> columns) {
        if (columns == null) {
            return Lists.newArrayList();
        }
        return columns.stream().map(c -> toColumnDefinition(table, c)).collect(Collectors.toList());
    }

    protected ColumnDefinition toColumnDefinition(Table table, Column c) {
        String id = c.getId();
        List<Property> all = Lists.newArrayList();
        Property property = null;
        if (StringUtils.isNotBlank(id)) {
            property = new Property(ColumnPropertyDefaultKey.uuid.name(), id);
            all.add(property);
        }
        List<BaseClientProperty> properties = c.getProperties();
        if (properties != null) {
            List<Property> columnProperty = toProperty(table, properties);
            all.addAll(columnProperty);
        }
        return ColumnDefinition.builder()
            .colName(new Identifier(c.getName()))
            .comment(new Comment(c.getComment())).dataType(getDataType(c))
            .notNull(BooleanUtils.isFalse(c.isNullable()))
            .primary(c.isPrimaryKey())
            .properties(all).build();
    }

    /**
     * get DataType
     *
     * @param column
     * @return
     */
    protected abstract BaseDataType getDataType(Column column);

    /**
     * to property
     *
     * @param table
     * @param properties
     * @return
     */
    protected List<Property> toProperty(Table table, List<BaseClientProperty> properties) {
        if (properties == null) {
            return new ArrayList<>();
        }
        return properties.stream().map(p -> new Property(p.getKey(), p.valueString())).collect(Collectors.toList());
    }

    /**
     * to partition by
     *
     * @param table
     * @param columns
     * @return
     */
    protected PartitionedBy toPartitionedBy(Table table, List<Column> columns) {
        if (CollectionUtils.isEmpty(columns)) {
            return null;
        }
        List<ColumnDefinition> collect = columns.stream().filter(Column::isPartitionKey).sorted(Comparator.comparing(Column::getPartitionKeyIndex))
            .map(c -> ColumnDefinition.builder().colName(new Identifier(c.getName())).comment(new Comment(c.getComment())).dataType(getDataType(c))
                .notNull(BooleanUtils.isFalse(c.isNullable())).primary(c.isPrimaryKey()).build()).collect(Collectors.toList());
        return new PartitionedBy(collect);
    }

    /**
     * to constraint
     *
     * @param columns
     * @param constraints
     * @return
     */
    protected List<BaseConstraint> toConstraint(List<Column> columns, List<Constraint> constraints) {
        List<BaseConstraint> constraintList = Lists.newArrayList();
        BaseConstraint primaryConstraintByColumns = setPrimaryConstraintColumns(columns);
        if (primaryConstraintByColumns != null) {
            constraintList.add(primaryConstraintByColumns);
        }
        if (CollectionUtils.isEmpty(constraints)) {
            return constraintList;
        }
        for (Constraint c : constraints) {
            Identifier constraintName;
            if (StringUtils.isBlank(c.getName())) {
                constraintName = IdentifierUtil.sysIdentifier();
            } else {
                constraintName = new Identifier(c.getName());
            }
            ConstraintType constraintType = c.getType();
            if (constraintType == OutlineConstraintType.PRIMARY_KEY) {
                PrimaryConstraint primaryConstraint = new PrimaryConstraint(constraintName,
                    c.getColumns().stream().map(Identifier::new).collect(Collectors.toList()));
                constraintList.add(primaryConstraint);
            } else if (constraintType == OutlineConstraintType.UNIQUE) {
                UniqueConstraint primaryConstraint = new UniqueConstraint(constraintName,
                    c.getColumns().stream().map(Identifier::new).collect(Collectors.toList()));
                constraintList.add(primaryConstraint);
            }
        }
        return constraintList;
    }

    protected BaseConstraint setPrimaryConstraintColumns(List<Column> columns) {
        if (CollectionUtils.isEmpty(columns)) {
            return null;
        }
        List<Column> primaryKeyColumns = columns.stream().filter(
            c -> BooleanUtils.isTrue(c.isPrimaryKey())
        ).collect(Collectors.toList());
        BaseConstraint baseConstraint = null;
        if (primaryKeyColumns.size() > 1) {
            List<Identifier> list = new ArrayList<>();
            for (Column column : primaryKeyColumns) {
                list.add(new Identifier(column.getName()));
                column.setPrimaryKey(false);
                column.setNullable(false);
            }
            baseConstraint = new PrimaryConstraint(IdentifierUtil.sysIdentifier(), list);
        }
        return baseConstraint;
    }

    protected Column getColumn(ColumnDefinition c, boolean partitionKey, Integer partitionKeyIndex) {
        BaseDataType dataType = c.getDataType();
        Column column = Column.builder()
            .id(c.getColName().getValue())
            .name(c.getColName().getValue())
            .comment(c.getCommentValue())
            .id(c.getColName().getValue())
            .dataType(dataType.getTypeName().getValue())
            .nullable(BooleanUtils.isNotTrue(c.getNotNull()))
            .primaryKey(BooleanUtils.isTrue(c.getPrimary()))
            .partitionKey(partitionKey)
            .partitionKeyIndex(partitionKeyIndex).build();
        IDataTypeName typeName = dataType.getTypeName();
        Dimension dimension = typeName.getDimension();
        if (dimension == Dimension.MULTIPLE) {
            //如果是多纬度的类型，直接设置类型文本
            column.setDataType(dataType.getOrigin());
            return column;
        }
        if (!(dataType instanceof GenericDataType)) {return column;}
        GenericDataType genericDataType = (GenericDataType)dataType;
        List<DataTypeParameter> arguments = genericDataType.getArguments();
        //if only one
        if (dimension == Dimension.TWO) {
            //because is decimal, so must type parameter is numeric
            if (arguments.size() == FIRST_INDEX) {
                DataTypeParameter dataTypeParameter = arguments.get(0);
                NumericParameter numericParameter = (NumericParameter)dataTypeParameter;
                column.setPrecision(Integer.parseInt(numericParameter.getValue()));
            } else if (arguments.size() == SECOND_INDEX) {
                DataTypeParameter dataTypeParameter = arguments.get(0);
                NumericParameter numericParameter = (NumericParameter)dataTypeParameter;
                column.setPrecision(Integer.parseInt(numericParameter.getValue()));
                dataTypeParameter = arguments.get(1);
                numericParameter = (NumericParameter)dataTypeParameter;
                column.setScale(Integer.parseInt(numericParameter.getValue()));
            }
        } else if (dimension == Dimension.ONE) {
            if (arguments.size() == FIRST_INDEX) {
                DataTypeParameter dataTypeParameter = arguments.get(0);
                NumericParameter numericParameter = (NumericParameter)dataTypeParameter;
                column.setLength(Integer.parseInt(numericParameter.getValue()));
            }
        }
        return column;
    }

    protected void setPrimaryKey(List<BaseConstraint> constraints, Column column) {
        //如果已经是主键，那么就不用再去读了
        if (column.isPrimaryKey()) {
            return;
        }
        boolean primaryKey = toPrimaryKey(column, constraints);
        //如果约束也不是主键，那么直接返回
        if (!primaryKey) {
            return;
        }
        //设置主键和非空
        column.setPrimaryKey(true);
        column.setNullable(false);
    }

    /**
     * 从约束中获取主键信息
     *
     * @param column
     * @param constraints
     * @return
     */
    protected boolean toPrimaryKey(Column column, List<BaseConstraint> constraints) {
        if (CollectionUtils.isEmpty(constraints)) {
            return false;
        }
        Optional<BaseConstraint> baseConstraintOptional = constraints.stream().filter(c -> c instanceof PrimaryConstraint).findFirst();
        if (!baseConstraintOptional.isPresent()) {
            return false;
        }
        PrimaryConstraint baseConstraint = (PrimaryConstraint)baseConstraintOptional.get();
        return baseConstraint.getColNames().stream().anyMatch(
            x -> Objects.equals(x, new Identifier(column.getName()))
        );
    }

    public final boolean contains(List<ColumnDefinition> list, Column column) {
        return list.stream().anyMatch(c -> Objects.equals(new Identifier(column.getName()), c.getColName()));
    }

    public final boolean contains(List<Column> list, ColumnDefinition columnDefinition) {
        return list.stream().anyMatch(c -> Objects.equals(new Identifier(c.getName()), columnDefinition.getColName()));
    }

}
