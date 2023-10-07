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

package com.aliyun.fastmodel.core.tree.statement.domain;

import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.statement.BaseDrop;
import com.aliyun.fastmodel.core.tree.statement.constants.StatementType;

/**
 * 删除数据域的语句
 * <p>
 * DSL举例
 * <blockquote><pre>
 * DROP DOMAIN ut.domain_name
 * </pre> </blockquote>
 *
 * @author panguanjing
 * @date 2020/9/7
 */
public class DropDomain extends BaseDrop {

    public DropDomain(QualifiedName qualifiedName) {
        super(qualifiedName);
        setStatementType(StatementType.DOMAIN);
    }
}
