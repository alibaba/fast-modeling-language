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

package com.aliyun.fastmodel.core.tree.statement.group;

import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.statement.BaseCreate;
import com.aliyun.fastmodel.core.tree.statement.constants.StatementType;
import com.aliyun.fastmodel.core.tree.statement.element.CreateElement;
import lombok.Getter;

/**
 * 创建分组
 * <p>
 * 创建分组内容
 * <pre>
 *     CREATE GROUP DICT a.b COMMENT 'bc' WITH('param1'='value1')
 * </pre>
 *
 * @author panguanjing
 * @date 2021/1/8
 */
@Getter
public class CreateGroup extends BaseCreate {

    private final GroupType groupType;

    public CreateGroup(CreateElement element,
                       GroupType groupType) {
        super(element);
        this.groupType = groupType;
        setStatementType(StatementType.GROUP);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCreateGroup(this, context);
    }
}
