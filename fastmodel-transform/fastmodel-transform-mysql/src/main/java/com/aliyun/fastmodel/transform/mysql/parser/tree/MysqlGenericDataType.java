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

import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.core.tree.NodeLocation;
import com.aliyun.fastmodel.core.tree.datatype.DataTypeParameter;
import com.aliyun.fastmodel.core.tree.datatype.GenericDataType;
import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.transform.api.format.DefaultExpressionVisitor;
import lombok.Getter;
import org.apache.commons.lang3.BooleanUtils;

/**
 * MySQL通用数据类型，支持 UNSIGNED / SIGNED / ZEROFILL 等数值类型修饰符
 *
 * <p>MySQL 的数值类型支持无符号、零填充等修饰符，这些语义不属于基础类型名称。
 * 如果反向解析时直接丢弃，会导致 INT UNSIGNED 被渲染成 INT，数值范围收窄。
 * 这里单独承载修饰符，并保证渲染时能够还原。</p>
 *
 * @author panguanjing
 */
@Getter
public class MysqlGenericDataType extends GenericDataType {

    /**
     * 数值符号修饰符
     */
    public enum SignEnum {
        /**
         * 无符号
         */
        UNSIGNED,
        /**
         * 有符号
         */
        SIGNED
    }

    private static final DefaultExpressionVisitor PARAMETER_VISITOR = new DefaultExpressionVisitor();

    private final SignEnum signEnum;

    private final Boolean zeroFill;

    public MysqlGenericDataType(NodeLocation location, String origin, String dataTypeName,
        List<DataTypeParameter> arguments, SignEnum signEnum, Boolean zeroFill) {
        super(location, origin, dataTypeName, arguments);
        this.signEnum = signEnum;
        this.zeroFill = zeroFill;
    }

    public MysqlGenericDataType(Identifier name, List<DataTypeParameter> arguments,
        SignEnum signEnum, Boolean zeroFill) {
        super(name, arguments);
        this.signEnum = signEnum;
        this.zeroFill = zeroFill;
    }

    @Override
    public IDataTypeName getTypeName() {
        return MysqlDataTypeName.getByValue(format());
    }

    /**
     * 按照 MySQL 语法渲染类型，修饰符位于参数之后，例如 INT(10) UNSIGNED ZEROFILL
     *
     * @return 类型字符串
     */
    public String format() {
        StringBuilder result = new StringBuilder(getName());
        if (getArguments() != null && !getArguments().isEmpty()) {
            result.append(getArguments().stream()
                .map(parameter -> PARAMETER_VISITOR.process(parameter, (Void)null))
                .collect(Collectors.joining(",", "(", ")")));
        }
        if (signEnum != null) {
            result.append(" ").append(signEnum.name());
        }
        if (BooleanUtils.isTrue(zeroFill)) {
            result.append(" ZEROFILL");
        }
        return result.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (!super.equals(o)) {
            return false;
        }
        MysqlGenericDataType that = (MysqlGenericDataType)o;
        return signEnum == that.signEnum && Objects.equals(zeroFill, that.zeroFill);
    }

    @Override
    public int hashCode() {
        List<String> argumentValues = getArguments() == null ? null : getArguments().stream()
            .map(parameter -> PARAMETER_VISITOR.process(parameter, (Void)null).toUpperCase(Locale.ROOT))
            .collect(Collectors.toList());
        return Objects.hash(getTypeName().getValue().toUpperCase(Locale.ROOT), argumentValues, signEnum, zeroFill);
    }

    @Override
    public String toString() {
        return format();
    }
}
