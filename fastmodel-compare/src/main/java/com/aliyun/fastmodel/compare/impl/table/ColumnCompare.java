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

package com.aliyun.fastmodel.compare.impl.table;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.compare.util.CompareUtil;
import com.aliyun.fastmodel.compare.impl.table.column.OrderColumnManager;
import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.core.tree.Comment;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.constants.ColumnCategory;
import com.aliyun.fastmodel.core.tree.statement.constants.ColumnPropertyDefaultKey;
import com.aliyun.fastmodel.core.tree.statement.table.AddCols;
import com.aliyun.fastmodel.core.tree.statement.table.ChangeCol;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition;
import com.aliyun.fastmodel.core.tree.statement.table.ColumnDefinition.ColumnBuilder;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.core.tree.statement.table.DropCol;
import com.aliyun.fastmodel.core.tree.statement.table.SetColumnOrder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * 列的比对操作处理
 *
 * @author panguanjing
 * @date 2021/8/30
 */
public class ColumnCompare implements TableElementCompare {

    @Override
    public List<BaseStatement> compareTableElement(CreateTable before, CreateTable after) {
        List<ColumnDefinition> beforeColumnDefines = before.getColumnDefines();
        List<ColumnDefinition> afterColumnDefines = after.getColumnDefines();

        boolean beforeEmpty = beforeColumnDefines == null || beforeColumnDefines.isEmpty();
        boolean afterNotEmpty = afterColumnDefines != null && !afterColumnDefines.isEmpty();
        if (beforeEmpty && !afterNotEmpty) {
            return ImmutableList.of();
        }
        QualifiedName qualifiedName = after.getQualifiedName();
        if (beforeEmpty && afterNotEmpty) {
            AddCols addCols = new AddCols(
                qualifiedName,
                afterColumnDefines
            );
            return ImmutableList.of(addCols);
        }
        //如果after的列都是空的
        if (!afterNotEmpty) {
            List<DropCol> dropCols = beforeColumnDefines.stream().map(
                x -> new DropCol(qualifiedName, x.getColName())).collect(Collectors.toList());
            return ImmutableList.copyOf(dropCols);
        }

        Builder<BaseStatement> builder
            = incrementCompare(qualifiedName, beforeColumnDefines, afterColumnDefines, (q, c) -> {
            return new DropCol(q, c.getColName());
        }, (q, c) -> {
            AddCols addCols = new AddCols(q, c);
            return ImmutableList.of(addCols);
        });

        //顺序的调整
        //copy beforeColumn
        List<ColumnDefinition> beforeCopy = new CopyOnWriteArrayList<>(beforeColumnDefines);
        OrderColumnManager beforeOrderColumnManager = new OrderColumnManager(qualifiedName, beforeCopy);
        OrderColumnManager afterOrderColumnManager = new OrderColumnManager(qualifiedName, afterColumnDefines);
        for (ColumnDefinition c : afterColumnDefines) {
            SetColumnOrder compare = beforeOrderColumnManager.compare(c.getColName(), afterOrderColumnManager);
            if (compare == null) {
                continue;
            }
            //if change then add and apply compare
            builder.add(compare);
            beforeOrderColumnManager.apply(compare);
        }
        return builder.build();
    }

    protected Builder<BaseStatement> incrementCompare(QualifiedName qualifiedName,
                                                      List<ColumnDefinition> beforeColumnDefines,
                                                      List<ColumnDefinition> afterColumnDefines,
                                                      BiFunction<QualifiedName, ColumnDefinition, BaseStatement> addDrop,
                                                      BiFunction<QualifiedName, List<ColumnDefinition>,
                                                          List<BaseStatement>> addCols) {
        Builder<BaseStatement> builder = ImmutableList.builder();
        //如果两边的都有的话内容
        List<ColumnDefinition> afterCopy = Lists.newArrayList(afterColumnDefines);
        Map<String, ColumnDefinition> afterMapColumn = uuidMap(afterColumnDefines);
        Map<String, ColumnDefinition> afterColMap = colMap(afterCopy);
        for (ColumnDefinition c : beforeColumnDefines) {
            if (afterColumnDefines.contains(c)) {
                afterCopy.remove(c);
            } else {
                Optional<String> uuid = uuid(c);
                if (uuid.isPresent() && afterMapColumn.get(uuid.get()) != null) {
                    ColumnDefinition afterColumnDefine = afterMapColumn.get(uuid.get());
                    ChangeCol statement = compare(qualifiedName, c, afterColumnDefine);
                    if (statement != null) {
                        builder.add(statement);
                    }
                    afterCopy.remove(afterColumnDefine);
                } else {
                    ColumnDefinition columnDefinition = afterColMap.get(c.getColName().getValue().toLowerCase());
                    if (columnDefinition != null) {
                        ChangeCol element = compare(qualifiedName, c, columnDefinition);
                        if (element != null) {
                            builder.add(element);
                        }
                        afterCopy.remove(columnDefinition);
                    } else {
                        BaseStatement apply = addDrop.apply(qualifiedName, c);
                        builder.add(apply);
                    }
                }
            }
        }
        if (!afterCopy.isEmpty()) {
            List<BaseStatement> apply = addCols.apply(qualifiedName, afterCopy);
            builder.addAll(apply);
        }
        return builder;
    }

    /**
     * 根据列进行转换
     *
     * @param afterCopy
     * @return {@code }
     */
    private Map<String, ColumnDefinition> colMap(List<ColumnDefinition> afterCopy) {
        Map<String, ColumnDefinition> map = new HashMap<>(afterCopy.size());
        for (ColumnDefinition c : afterCopy) {
            map.put(c.getColName().getValue().toLowerCase(), c);
        }
        return map;
    }

    /**
     * 将column转换，uuid的map转换
     *
     * @param beforeColumnDefines
     * @return
     */
    private Map<String, ColumnDefinition> uuidMap(List<ColumnDefinition> beforeColumnDefines) {
        Map<String, ColumnDefinition> columnDefinitionMap = new HashMap<>(20);
        for (ColumnDefinition c : beforeColumnDefines) {
            List<Property> columnProperties = c.getColumnProperties();
            if (columnProperties == null || columnProperties.isEmpty()) {
                continue;
            }
            Optional<Property> value = columnProperties.stream().filter(x -> x.getName().equalsIgnoreCase(
                    ColumnPropertyDefaultKey.uuid
                        .name()))
                .findFirst();
            value.ifPresent(x -> {
                columnDefinitionMap.put(x.getValue(), c);
            });
        }
        return columnDefinitionMap;
    }

    /**
     * 统一使用changeCol语句进行处理
     *
     * @param qualifiedName
     * @param beforeDefine
     * @param afterDefine
     * @return
     */
    private ChangeCol compare(QualifiedName qualifiedName, ColumnDefinition beforeDefine,
                              ColumnDefinition afterDefine) {
        boolean changeComment = isChangeColumnComment(beforeDefine, afterDefine);
        boolean changeAliased = !Objects.equals(beforeDefine.getAliasedName(), afterDefine.getAliasedName());
        boolean changePrimary = false;
        boolean changeColumn = !StringUtils.equalsIgnoreCase(beforeDefine.getColName().getValue(),
            afterDefine.getColName().getValue());
        boolean changeNotNull = false;
        boolean changeType = !Objects.equals(beforeDefine.getDataType(), afterDefine.getDataType());
        boolean changeCategory = isChangeColumnCategory(beforeDefine, afterDefine);
        boolean changeDim = isChangeDim(beforeDefine, afterDefine);
        boolean changeRefIndicators = isChangeRefIndicaotrs(beforeDefine, afterDefine);
        List<Property> changeProperties = changeProperties(beforeDefine, afterDefine);
        Comment comment = null;
        if (changeComment) {
            if (afterDefine.getComment() == null) {
                comment = new Comment(StringUtils.EMPTY);
            } else {
                comment = afterDefine.getComment();
            }
        }
        ColumnBuilder builder = ColumnDefinition.builder();
        builder.colName(afterDefine.getColName()).dataType(afterDefine.getDataType()).comment(comment);
        if (changeAliased) {
            builder.aliasedName(afterDefine.getAliasedName());
        }
        Boolean primary = beforeDefine.getPrimary();
        Boolean afterDefinePrimary = afterDefine.getPrimary();
        if (!BooleanUtils.isNotTrue(primary) || !BooleanUtils.isNotTrue(afterDefinePrimary)) {
            if (!Objects.equals(primary, afterDefinePrimary)) {
                builder.primary(beforeDefine == null ? afterDefinePrimary : BooleanUtils.negate(primary));
                changePrimary = true;
            }
        }
        Boolean beforeDefineNotNull = beforeDefine.getNotNull();
        Boolean afterDefineNotNull = afterDefine.getNotNull();
        if (!BooleanUtils.isNotTrue(beforeDefineNotNull) || !BooleanUtils.isNotTrue(afterDefineNotNull)) {
            if (!Objects.equals(afterDefineNotNull, beforeDefineNotNull)) {
                builder.notNull(
                    beforeDefineNotNull == null ? afterDefineNotNull : BooleanUtils.negate(beforeDefineNotNull));
                changeNotNull = true;
            }
        }
        //如果都没有发生修改，那么直接返回null
        if (!changeComment && !changeAliased && !changePrimary && !changeNotNull && !changeColumn && !changeType
            && !changeCategory && changeProperties.isEmpty() && !changeDim && !changeRefIndicators) {
            return null;
        }
        if (changeCategory) {
            builder.category(afterDefine.getCategory() == null ? ColumnCategory.ATTRIBUTE : afterDefine.getCategory());
        }
        if (!changeProperties.isEmpty()) {
            builder.properties(changeProperties);
        }
        if (changeDim) {
            builder.refDimension(afterDefine.getRefDimension());
        }
        if (changeRefIndicators) {
            builder.refIndicators(afterDefine.getRefIndicators());
        }
        ChangeCol changeCol = new ChangeCol(qualifiedName, beforeDefine.getColName(), builder.build());
        return changeCol;
    }

    private boolean isChangeDim(ColumnDefinition beforeDefine, ColumnDefinition afterDefine) {
        QualifiedName beforeDim = beforeDefine.getRefDimension();
        QualifiedName afterDim = afterDefine.getRefDimension();
        return !Objects.equals(beforeDim, afterDim);
    }

    private boolean isChangeRefIndicaotrs(ColumnDefinition beforeDefine, ColumnDefinition afterDefine) {
        List<Identifier> beforeDim = beforeDefine.getRefIndicators();
        List<Identifier> afterDim = afterDefine.getRefIndicators();
        return CompareUtil.isChangeCollection(beforeDim, afterDim);
    }

    private boolean isChangeColumnCategory(ColumnDefinition beforeDefine, ColumnDefinition afterDefine) {
        if (beforeDefine.getCategory() == afterDefine.getCategory()) {
            return false;
        }
        if (beforeDefine.getCategory() == null && afterDefine.getCategory() == ColumnCategory.ATTRIBUTE) {
            return false;
        }
        if (beforeDefine.getCategory() == ColumnCategory.ATTRIBUTE && afterDefine.getCategory() == null) {
            return false;
        }
        return true;
    }

    private boolean isChangeColumnComment(ColumnDefinition beforeDefine, ColumnDefinition afterDefine) {
        Comment comment = beforeDefine.getComment();
        Comment afterComment = afterDefine.getComment();
        return isChangeComment(comment, afterComment);
    }

    private boolean isChangeComment(Comment comment, Comment afterComment) {
        if (comment == null && afterComment == null) {
            return false;
        }
        if (comment == null && afterComment != null) {
            return StringUtils.isNotBlank(afterComment.getComment());
        }
        if (comment != null && afterComment == null) {
            return StringUtils.isNotBlank(comment.getComment());
        }
        return !StringUtils.equals(StringUtils.trimToEmpty(comment.getComment()),
            StringUtils.trimToEmpty(afterComment.getComment()));
    }

    /**
     * 获取UUID信息
     *
     * @param columnDefinition
     * @return
     */
    private Optional<String> uuid(ColumnDefinition columnDefinition) {
        List<Property> columnProperties = columnDefinition.getColumnProperties();
        if (columnProperties == null || columnProperties.isEmpty()) {
            return Optional.empty();
        }
        Optional<Property> uuid = columnProperties.stream().filter(x -> x.getName().equalsIgnoreCase(
                ColumnPropertyDefaultKey.uuid.name()))
            .findFirst();
        if (uuid.isPresent()) {
            return Optional.of(uuid.get().getValue());
        } else {
            return Optional.empty();
        }
    }

    private List<Property> changeProperties(ColumnDefinition beforeDefine, ColumnDefinition afterDefine) {
        List<Property> properties = beforeDefine.getColumnProperties();
        List<Property> afterProperties = afterDefine.getColumnProperties();
        boolean isBeforeEmpty = beforeDefine.isPropertyEmpty();
        boolean isAfterEmpty = afterDefine.isPropertyEmpty();
        if (isBeforeEmpty && isAfterEmpty) {
            return ImmutableList.of();
        }
        //如果原来为空，目标不是空，那么直接设置
        if (isBeforeEmpty && !isAfterEmpty) {
            return afterProperties;
        }
        if (isAfterEmpty) {
            return properties.stream().map(x -> {
                return new Property(x.getName(), StringUtils.EMPTY);
            }).collect(Collectors.toList());
        } else {
            List<Property> unSet = new ArrayList<>();
            for (Property b : properties) {
                String name = b.getName();
                if (!contains(name, afterProperties)) {
                    unSet.add(b);
                }
            }
            List<Property> setProp = new ArrayList<>();
            for (Property ap : afterProperties) {
                if (properties.contains(ap)) {
                    continue;
                } else {
                    setProp.add(ap);
                }
            }
            List<Property> list = new ArrayList<>();
            if (!unSet.isEmpty()) {
                list.addAll(unSet.stream().map(x -> {
                    return new Property(x.getName(), StringUtils.EMPTY);
                }).collect(Collectors.toList()));
            }

            if (!setProp.isEmpty()) {
                list.addAll(setProp);
            }
            return list;
        }
    }

    private boolean contains(String name, List<Property> afterProperties) {
        Map<String, Property> maps = Maps.newHashMap();
        for (Property p : afterProperties) {
            maps.put(p.getName().toLowerCase(), p);
        }
        return maps.containsKey(name);
    }

}
