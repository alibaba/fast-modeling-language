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

package com.aliyun.fastmodel.transform.postgresql.client.converter;

import com.aliyun.fastmodel.core.parser.LanguageParser;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName;
import com.aliyun.fastmodel.transform.api.client.PropertyConverter;
import com.aliyun.fastmodel.transform.api.client.dto.table.Column;
import com.aliyun.fastmodel.transform.postgresql.context.PostgreSQLTransformContext;
import com.aliyun.fastmodel.transform.postgresql.format.PostgreSQLOutVisitor;
import com.aliyun.fastmodel.transform.postgresql.parser.PostgreSQLLanguageParser;
import com.aliyun.fastmodel.transform.postgresql.parser.tree.datatype.PostgreSQLDataTypeName;

/**
 * 标准 PostgreSQL 的 ClientConverter 具体实现
 *
 * @author panguanjing
 * @date 2026/3/31
 */
public class PostgreSQLStandaloneClientConverter extends PostgreSQLClientConverter<PostgreSQLTransformContext> {

    private final PostgreSQLLanguageParser languageParser = new PostgreSQLLanguageParser();

    private final PostgreSQLPropertyConverter propertyConverter = new PostgreSQLPropertyConverter();

    @Override
    public IDataTypeName getDataTypeName(String dataTypeName) {
        return PostgreSQLDataTypeName.getByValue(dataTypeName);
    }

    @Override
    public String getRaw(Node node) {
        PostgreSQLOutVisitor visitor = new PostgreSQLOutVisitor(PostgreSQLTransformContext.builder().build());
        node.accept(visitor, 0);
        return visitor.getBuilder().toString();
    }

    @Override
    public LanguageParser getLanguageParser() {
        return languageParser;
    }

    @Override
    public BaseDataType getDataType(Column column) {
        String dataType = column.getDataType();
        if (dataType == null) {
            return null;
        }
        return languageParser.parseDataType(dataType, null);
    }

    @Override
    public PropertyConverter getPropertyConverter() {
        return propertyConverter;
    }
}
