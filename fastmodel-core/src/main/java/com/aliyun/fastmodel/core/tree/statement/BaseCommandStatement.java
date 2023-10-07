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

package com.aliyun.fastmodel.core.tree.statement;

import java.util.List;

import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.BaseFmlStatement;
import com.aliyun.fastmodel.core.tree.NodeLocation;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.statement.constants.StatementType;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * 基本的命令式语句, 比如导入命令、导出命令、渲染命令等
 *
 * @author panguanjing
 * @date 2021/11/30
 */
@Getter
@EqualsAndHashCode(callSuper = false)
public abstract class BaseCommandStatement extends BaseFmlStatement {

    protected final List<Property> properties;

    public BaseCommandStatement(List<Property> properties) {
        this(null, null, properties);
    }

    public BaseCommandStatement(NodeLocation nodeLocation,
                                List<Property> properties) {
        super(nodeLocation);
        this.properties = properties;
        setStatementType(StatementType.COMMAND);

    }

    public BaseCommandStatement(NodeLocation location, String origin,
                                List<Property> properties) {
        super(location, origin);
        this.properties = properties;
        setStatementType(StatementType.COMMAND);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitBaseCommandStatement(this, context);
    }
}
