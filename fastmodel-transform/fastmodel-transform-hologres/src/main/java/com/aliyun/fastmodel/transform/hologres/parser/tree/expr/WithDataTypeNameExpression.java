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

package com.aliyun.fastmodel.transform.hologres.parser.tree.expr;

import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.core.tree.NodeLocation;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.transform.hologres.parser.visitor.HologresVisitor;
import lombok.Getter;

/**
 * with dataTypeName Expression
 *
 * @author panguanjing
 * @date 2024/4/11
 */
@Getter
public class WithDataTypeNameExpression extends BaseExpression {

    private final BaseExpression baseExpression;

    private final BaseDataType baseDataType;

    public WithDataTypeNameExpression(NodeLocation location, BaseExpression baseExpression, BaseDataType dataTypeName) {
        super(location);
        this.baseExpression = baseExpression;
        this.baseDataType = dataTypeName;
    }

    public WithDataTypeNameExpression(NodeLocation location, String origin, BaseExpression baseExpression, BaseDataType dataTypeName) {
        super(location, origin);
        this.baseExpression = baseExpression;
        this.baseDataType = dataTypeName;
    }

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        HologresVisitor<R, C> hologresVisitor = (HologresVisitor<R, C>)visitor;
        return hologresVisitor.visitWithDataTypeNameExpression(this, context);
    }
}
