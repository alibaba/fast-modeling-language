/*
 * Copyright [2024] [name of copyright owner]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aliyun.fastmodel.transform.hive.parser.visitor;

import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.transform.hive.parser.tree.datatype.HiveGenericDataType;

/**
 * visit begin work
 *
 * @author panguanjing
 * @date 2022/6/9
 */
public interface HiveVisitor<R, C> extends IAstVisitor<R, C> {

    default R visitHiveGenericDataType(HiveGenericDataType hiveGenericDataType, C context) {
        return visitGenericDataType(hiveGenericDataType, context);
    }
}
