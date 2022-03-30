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

package com.aliyun.fastmodel.transform.api.domain.factory.impl;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.BaseConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.DimConstraint;
import com.aliyun.fastmodel.core.tree.statement.table.constraint.PrimaryConstraint;
import com.aliyun.fastmodel.core.tree.util.PropertyUtil;
import com.aliyun.fastmodel.transform.api.domain.factory.DomainFactory;
import com.aliyun.fastmodel.transform.api.domain.table.ColDataModel;
import com.aliyun.fastmodel.transform.api.domain.table.ConstraintDataModel;
import com.aliyun.fastmodel.transform.api.domain.table.TableColModel;
import com.aliyun.fastmodel.transform.api.domain.table.TableDataModel;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;

/**
 * TableDomainFactory
 *
 * @author panguanjing
 * @date 2021/12/12
 */
public class TableDomainFactory implements DomainFactory<CreateTable, TableDataModel> {
    @Override
    public TableDataModel create(CreateTable statement) {
        return toTableDataModel(statement);
    }

    private TableDataModel toTableDataModel(CreateTable createTableStatement) {
        TableDataModel dataModel = new TableDataModel();
        dataModel.setDatabase(createTableStatement.getBusinessUnit());
        dataModel.setTableCode(createTableStatement.getIdentifier());
        String aliasedNameValue = createTableStatement.getAliasedNameValue();
        dataModel.setTableName(aliasedNameValue);
        Comment comment = createTableStatement.getComment();
        dataModel.setComment(comment != null ? comment.getComment() : null);
        dataModel.setTblProperties(PropertyUtil.toMap(createTableStatement.getProperties()));
        dataModel.setTableType(createTableStatement.getTableType());
        List<ColumnDefinition> columnDefines = createTableStatement.getColumnDefines();
        Map<String, ColDataModel> map = new LinkedHashMap<>();
        List<ColDataModel> all = Lists.newArrayList();
        if (!createTableStatement.isColumnEmpty()) {
            List<ColDataModel> list = getColDataModels(columnDefines,
                map);
            all.addAll(list);
        }
        if (!createTableStatement.isPartitionEmpty()) {
            List<ColDataModel> list = getColDataModels(createTableStatement.getPartitionedBy().getColumnDefinitions(),
                map);
            list.stream().forEach(c -> {
                c.setIsPartitionKey(true);
            });
            all.addAll(list);
        }
        dataModel.setCols(all);
        List<BaseConstraint> constraintStatements = createTableStatement.getConstraintStatements();
        List<ConstraintDataModel> constraintDataModels = new ArrayList<>();
        if (createTableStatement.isConstraintEmpty()) {return dataModel;}
        for (BaseConstraint baseConstraint : constraintStatements) {
            if (baseConstraint instanceof PrimaryConstraint) {
                PrimaryConstraint primaryConstraintStatement
                    = (PrimaryConstraint)baseConstraint;
                List<String> colNames = primaryConstraintStatement.getColNames().stream().map(Identifier::getValue)
                    .collect(
                        Collectors.toList());
                for (String col : colNames) {
                    map.get(col).setPrimaryKey(true);
                }
            }
            if (baseConstraint instanceof DimConstraint) {
                DimConstraint dimConstraintStatement = (DimConstraint)baseConstraint;
                String identifier = createTableStatement.getIdentifier();
                List<Identifier> colNames = dimConstraintStatement.getColNames();
                QualifiedName right = dimConstraintStatement.getReferenceTable();
                List<Identifier> referenceColNames = dimConstraintStatement.getReferenceColNames();
                if (CollectionUtils.isNotEmpty(colNames)) {
                    List<ConstraintDataModel> collect = IntStream.range(0, colNames.size())
                        .mapToObj(x -> {
                            Identifier identifier1 = colNames.get(x);
                            Identifier identifier2 = referenceColNames.get(x);
                            return getConstraintDataModel(identifier, right,
                                identifier1, identifier2);
                        }).collect(Collectors.toList());
                    constraintDataModels.addAll(collect);
                } else {
                    ConstraintDataModel constraintDataModel = getConstraintDataModel(identifier,
                        dimConstraintStatement.getReferenceTable(), null, null);
                    constraintDataModels.add(constraintDataModel);
                }
            }
        }
        dataModel.setConstraints(constraintDataModels);
        return dataModel;
    }

    private List<ColDataModel> getColDataModels(List<ColumnDefinition> columnDefines, Map<String, ColDataModel> map) {
        List<ColDataModel> list = new ArrayList<>();
        for (ColumnDefinition columnDefine : columnDefines) {
            ColDataModel colDataModel = new ColDataModel();
            String colName = columnDefine.getColName().getValue();
            colDataModel.setColName(colName);
            colDataModel.setColComment(columnDefine.getCommentValue());
            colDataModel.setColAlias(columnDefine.getAliasValue());
            colDataModel.setNotNull(columnDefine.getNotNull());
            colDataModel.setPrimaryKey(columnDefine.getPrimary());
            BaseDataType dataType = columnDefine.getDataType();
            if (dataType != null) {
                colDataModel.setColType(dataType.toString());
            }
            list.add(colDataModel);
            map.put(colName, colDataModel);
        }
        return list;
    }

    private ConstraintDataModel getConstraintDataModel(String leftTable, QualifiedName right, Identifier leftColumn,
                                                       Identifier rightColumn) {
        TableColModel tableColModel = new TableColModel();
        tableColModel.setTable(leftTable);
        if (leftColumn != null) {
            tableColModel.setColumn(leftColumn.getValue());
        }
        TableColModel rightColModel = new TableColModel();
        rightColModel.setTable(right.getSuffix());
        if (rightColumn != null) {
            rightColModel.setColumn(rightColumn.getValue());
        }
        return ConstraintDataModel.builder()
            .left(tableColModel)
            .right(rightColModel)
            .build();
    }

}
