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

package com.aliyun.fastmodel.transform.fml.datatype;

import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.transform.api.datatype.DataTypeConverter;
import com.aliyun.fastmodel.transform.api.dialect.DialectMeta;
import com.aliyun.fastmodel.transform.api.dialect.DialectName;
import com.google.auto.service.AutoService;

/**
 * Fml 2 Oracle DataType converter
 *
 * @author panguanjing
 * @date 2021/8/18
 */
@AutoService(DataTypeConverter.class)
public class Fml2OracleDataTypeConverter implements DataTypeConverter {
    @Override
    public BaseDataType convert(BaseDataType baseDataType) {
        return baseDataType;
    }

    @Override
    public DialectMeta getSourceDialect() {
        return DialectMeta.getByName(DialectName.FML);
    }

    @Override
    public DialectMeta getTargetDialect() {
        return DialectMeta.getByName(DialectName.ORACLE);
    }
}
