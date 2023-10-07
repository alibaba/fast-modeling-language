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

package com.aliyun.fastmodel.transform.api.datatype;

import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.transform.api.dialect.DialectMeta;

/**
 * 目前在不同系统的转换的处理上类型的方式，是整个类型
 * 最大的部分内容.
 *
 * @author panguanjing
 * @date 2021/8/6
 */
public interface DataTypeConverter {
    /**
     * 将老的类型转为新的类型
     *
     * @param baseDataType
     * @return
     */
    BaseDataType convert(BaseDataType baseDataType);

    /**
     * 源Dialect
     *
     * @return {@link DialectMeta}
     */
    DialectMeta getSourceDialect();

    /**
     * 目标Dialect
     *
     * @return {@link DialectMeta}
     */
    DialectMeta getTargetDialect();
}
