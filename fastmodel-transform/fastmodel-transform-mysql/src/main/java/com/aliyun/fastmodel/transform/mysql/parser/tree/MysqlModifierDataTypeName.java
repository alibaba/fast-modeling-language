/*
 * Copyright 2026 Alibaba Group Holding Ltd.
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

package com.aliyun.fastmodel.transform.mysql.parser.tree;

import java.util.Objects;

import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName;
import com.aliyun.fastmodel.transform.api.datatype.simple.ISimpleDataTypeName;
import com.aliyun.fastmodel.transform.api.datatype.simple.SimpleDataTypeName;
import org.apache.commons.lang3.StringUtils;

/**
 * MySQL数据类型名称（带修饰符，例如 INT UNSIGNED / BIGINT UNSIGNED ZEROFILL）
 *
 * <p>MySQL 的无符号、零填充等属性并不属于基础类型名称，但外部系统（例如数据集成）
 * 传入的类型字符串会携带这些修饰符。这里用基础类型 + 修饰符的方式表达，
 * 保证 {@link #getValue()} 能够还原完整的类型语义，同时维度、简单类型等行为与基础类型保持一致。</p>
 *
 * @author panguanjing
 */
final class MysqlModifierDataTypeName implements ISimpleDataTypeName {

    private final MysqlDataTypeName base;

    private final String modifiers;

    MysqlModifierDataTypeName(MysqlDataTypeName base, String modifiers) {
        this.base = base;
        this.modifiers = modifiers;
    }

    @Override
    public SimpleDataTypeName getSimpleDataTypeName() {
        return base.getSimpleDataTypeName();
    }

    @Override
    public String getName() {
        return base.getName();
    }

    @Override
    public String getValue() {
        if (StringUtils.isBlank(modifiers)) {
            return base.getValue();
        }
        return base.getValue() + StringUtils.SPACE + modifiers;
    }

    @Override
    public Dimension getDimension() {
        return base.getDimension();
    }

    MysqlDataTypeName getBase() {
        return base;
    }

    String getModifiers() {
        return modifiers;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MysqlModifierDataTypeName that = (MysqlModifierDataTypeName)o;
        return base == that.base && Objects.equals(modifiers, that.modifiers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(base, modifiers);
    }

    @Override
    public String toString() {
        return getValue();
    }
}
