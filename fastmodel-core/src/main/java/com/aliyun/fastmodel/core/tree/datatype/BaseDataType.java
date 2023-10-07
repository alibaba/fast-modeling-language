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

package com.aliyun.fastmodel.core.tree.datatype;

import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.core.tree.NodeLocation;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;

/**
 * 列字段类型
 *
 * @author panguanjing
 * @date 2020/9/14
 */
public abstract class BaseDataType extends BaseExpression {

    public BaseDataType() {
        this(null, null);
    }

    public BaseDataType(NodeLocation location, String origin) {
        super(location, origin);
    }

    /**
     * 获取类型的名字
     *
     * @return 类型名字
     */
    public abstract IDataTypeName getTypeName();

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        return visitor.visitDataType(this, context);
    }
}
