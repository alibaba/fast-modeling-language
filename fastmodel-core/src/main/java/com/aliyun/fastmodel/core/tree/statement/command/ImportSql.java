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

package com.aliyun.fastmodel.core.tree.statement.command;

import java.util.List;
import java.util.Optional;

import com.aliyun.fastmodel.core.tree.AstVisitor;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.NodeLocation;
import com.aliyun.fastmodel.core.tree.Property;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.BaseCommandStatement;
import com.aliyun.fastmodel.core.tree.statement.constants.StatementType;
import com.google.common.collect.ImmutableList;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * 导入ImportSQL的命令格式
 *
 * @author panguanjing
 * @date 2021/12/1
 */
@Getter
@EqualsAndHashCode
public class ImportSql extends BaseCommandStatement {

    /**
     * 引擎方言
     */
    private final Identifier dialect;

    /**
     * 文件名
     */
    private final Optional<String> uri;

    /**
     * sql文本
     */
    private final Optional<String> sql;

    private final List<Property> properties;

    public ImportSql(Identifier dialect, Optional<String> uri, Optional<String> sql,
                     List<Property> properties) {
        this(null, null, dialect, uri, sql, properties);
    }

    public ImportSql(NodeLocation location, String origin, Identifier dialect,
                     Optional<String> uri, Optional<String> sql,
                     List<Property> properties) {
        super(location, origin, properties);
        this.dialect = dialect;
        this.uri = uri;
        this.sql = sql;
        this.properties = properties;
        setStatementType(StatementType.COMMAND);
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitImportSql(this, context);
    }
}
