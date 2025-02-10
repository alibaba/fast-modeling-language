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

package com.aliyun.fastmodel.transform.adbpg.parser.tree.partition.desc;

import java.util.List;

import com.aliyun.fastmodel.core.tree.AbstractNode;
import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;
import com.aliyun.fastmodel.core.tree.expr.BaseExpression;
import com.aliyun.fastmodel.transform.adbpg.parser.visitor.AdbPostgreSQLVisitor;
import lombok.Getter;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2024/10/17
 */
@Getter
public class PartitionValueExpression extends AbstractNode {

    private final BaseDataType baseDataType;

    private final BaseExpression baseExpression;

    public PartitionValueExpression(BaseDataType baseDataType, BaseExpression baseExpression) {
        this.baseDataType = baseDataType;
        this.baseExpression = baseExpression;
    }

    @Override
    public List<? extends Node> getChildren() {
        return List.of();
    }

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        AdbPostgreSQLVisitor<R, C> postgreSQLVisitor = (AdbPostgreSQLVisitor<R, C>)visitor;
        return postgreSQLVisitor.visitPartitionValueExpression(this, context);
    }
}
