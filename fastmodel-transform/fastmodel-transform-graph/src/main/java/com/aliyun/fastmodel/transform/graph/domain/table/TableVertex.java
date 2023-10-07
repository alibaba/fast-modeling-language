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

package com.aliyun.fastmodel.transform.graph.domain.table;

import java.util.Map;
import java.util.Set;

import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.statement.table.CreateTable;
import com.aliyun.fastmodel.transform.api.domain.factory.DomainFactorys;
import com.aliyun.fastmodel.transform.api.domain.table.TableDataModel;
import com.aliyun.fastmodel.transform.graph.domain.Vertex;
import com.aliyun.fastmodel.transform.graph.domain.VertexType;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * Table Vertex
 *
 * @author panguanjing
 * @date 2021/12/12
 */
public class TableVertex implements Vertex {

    public static final String NAME = "name";
    public static final String COLUMNS = "columns";
    public static final String COMMENT = "comment";

    private final TableDataModel tableDataModel;

    private final CreateTable creatTable;

    private final Map<String, Object> maps = Maps.newHashMap();

    public TableVertex(CreateTable createTable) {
        creatTable = createTable;
        tableDataModel = DomainFactorys.newTableFactory().create(createTable);
        if (CollectionUtils.isNotEmpty(tableDataModel.getCols())) {
            maps.put(COLUMNS, tableDataModel.getCols());
        }
        maps.put(COMMENT, tableDataModel.getComment());
        maps.put(NAME, tableDataModel.getTableName());
    }

    @Override
    @JsonProperty(value = "id")
    public String id() {
        if (StringUtils.isNotBlank(tableDataModel.getDatabase())) {
            return QualifiedName.of(tableDataModel.getDatabase(), tableDataModel.getTableCode()).toString();
        } else {
            return tableDataModel.getTableCode();
        }
    }

    @Override
    @JsonProperty(value = "label")
    public String label() {
        return StringUtils.isBlank(tableDataModel.getTableName()) ? tableDataModel.getTableCode()
            : tableDataModel.getTableName();
    }

    @Override
    @JsonProperty(value = "vertexType")
    public VertexType vertexType() {
        return TableVertexType.formTableType(creatTable.getTableDetailType().getParent());
    }

    @Override
    @JsonProperty(value = "attribute")
    public Map<String, Object> properties() {
        return maps;
    }

    @Override
    public <T> T getProperty(String property) {
        return (T)maps.get(property);
    }

    @Override
    public void setProperty(String key, Object value) {
        maps.put(key, value);
    }

    @Override
    public void removeProperty(String key) {
        maps.remove(key);
    }

    @Override
    @JsonIgnore
    public Set<String> getPropertyKeys() {
        return Sets.newHashSet(NAME, COLUMNS, COMMENT);
    }

}
