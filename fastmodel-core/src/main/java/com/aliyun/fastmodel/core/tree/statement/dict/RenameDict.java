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

package com.aliyun.fastmodel.core.tree.statement.dict;

import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.statement.BaseRename;
import com.aliyun.fastmodel.core.tree.statement.constants.StatementType;

/**
 * 修改数据字典名字
 * <p>
 * 以下是DSL举例：
 * <blockquote><pre>
 *      ALTER DICT ut.dict_name RENAME TO ut.dict_name2;
 * </pre></blockquote>
 *
 * @author panguanjing
 * @date 2020/11/13
 */
public class RenameDict extends BaseRename {

    public RenameDict(QualifiedName source,
                      QualifiedName target) {
        super(source, target);
        this.setStatementType(StatementType.DICT);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitRenameDict(this, context);
    }
}
