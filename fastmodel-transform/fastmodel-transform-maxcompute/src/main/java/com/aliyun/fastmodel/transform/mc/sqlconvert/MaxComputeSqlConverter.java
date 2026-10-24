/*
 * Copyright 2021 Alibaba Group Holding Ltd.
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

package com.aliyun.fastmodel.transform.mc.sqlconvert;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.statement.rule.function.BaseFunction;
import com.aliyun.fastmodel.core.tree.statement.rule.function.column.InTableFunction;
import com.aliyun.fastmodel.core.tree.statement.rule.function.table.TableFunction;
import com.aliyun.fastmodel.transform.api.dialect.Dialect;
import com.aliyun.fastmodel.transform.api.dialect.DialectName;
import com.aliyun.fastmodel.transform.api.sqlconvert.SqlConverter;
import com.aliyun.fastmodel.transform.mc.context.MaxComputeContext;
import com.google.auto.service.AutoService;
import org.apache.commons.collections.CollectionUtils;

/**
 * MaxComputeSqlConverter
 *
 * @author panguanjing
 * @date 2021/6/1
 */
@Dialect(value = DialectName.Constants.MAXCOMPUTE)
@AutoService(SqlConverter.class)
public class MaxComputeSqlConverter implements SqlConverter<BaseFunction, MaxComputeContext> {

    private static final Map<String, List<SqlConverter>> CONVERTER_MAP = new HashMap<>(4);

    static {
        add(InTableFunction.class, new NotInTableConverter());
        add(TableFunction.class, new UniqueKeyConverter());
    }

    public MaxComputeSqlConverter() {
    }

    @Override
    public String convert(QualifiedName tableName, BaseFunction baseFunction, String matchExpression, MaxComputeContext transformContext) {
        SqlConverter sqlConverter = find(baseFunction);
        if (sqlConverter == null) {
            throw new UnsupportedOperationException(
                "can't find the sql converter with function:" + baseFunction.funcName());
        }
        return sqlConverter.convert(tableName, baseFunction, matchExpression, transformContext);
    }

    private SqlConverter find(BaseFunction baseFunction) {
        List<SqlConverter> sqlConverters = CONVERTER_MAP.get(baseFunction.getClass().getName());
        if (CollectionUtils.isEmpty(sqlConverters)) {
            return null;
        }
        for (SqlConverter s : sqlConverters) {
            if (s.isMatch(baseFunction)) {
                return s;
            }
        }
        return null;
    }

    private static void add(Class<?> clazz, SqlConverter converter) {
        List<SqlConverter> sqlConverters = CONVERTER_MAP.get(clazz);
        if (sqlConverters == null) {
            sqlConverters = new ArrayList<>();
        }
        sqlConverters.add(converter);
        CONVERTER_MAP.put(clazz.getName(), sqlConverters);
    }
}
