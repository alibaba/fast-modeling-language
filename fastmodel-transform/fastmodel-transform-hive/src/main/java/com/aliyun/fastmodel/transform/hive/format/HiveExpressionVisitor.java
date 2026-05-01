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

package com.aliyun.fastmodel.transform.hive.format;

import com.aliyun.fastmodel.transform.api.format.DefaultExpressionVisitor;
import com.aliyun.fastmodel.transform.hive.parser.tree.datatype.HiveGenericDataType;
import com.aliyun.fastmodel.transform.hive.parser.visitor.HiveVisitor;

/**
 * HiveExpressionVisitor
 *
 * @author panguanjing
 * @date 2021/4/13
 */
public class HiveExpressionVisitor extends DefaultExpressionVisitor implements HiveVisitor<String, Void> {

    @Override
    public String visitHiveGenericDataType(HiveGenericDataType hiveGenericDataType, Void context) {
        return HiveVisitor.super.visitHiveGenericDataType(hiveGenericDataType, context);
    }
}
