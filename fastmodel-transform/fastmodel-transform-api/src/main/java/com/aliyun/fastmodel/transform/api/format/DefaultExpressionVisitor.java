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

package com.aliyun.fastmodel.transform.api.format;

import com.aliyun.fastmodel.core.formatter.ExpressionVisitor;
import com.aliyun.fastmodel.core.tree.datatype.GenericDataType;

/**
 * DefaultExpressionVisitor
 *
 * @author panguanjing
 * @date 2021/11/3
 */
public class DefaultExpressionVisitor extends ExpressionVisitor {
    @Override
    protected String getCustomDataTypeFormat(GenericDataType node) {
        return node.getName().toUpperCase();
    }
}
