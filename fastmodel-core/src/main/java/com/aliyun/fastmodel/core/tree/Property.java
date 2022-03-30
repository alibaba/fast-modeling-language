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

package com.aliyun.fastmodel.core.tree;

import java.util.List;

import com.aliyun.fastmodel.core.tree.expr.literal.BaseLiteral;
import com.aliyun.fastmodel.core.tree.expr.literal.StringLiteral;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2020/11/6
 */
@Getter
public class Property extends AbstractNode {

    private final String name;

    private final BaseLiteral valueLiteral;

    public Property(String name, String value) {
        this(name, new StringLiteral(value));
    }

    public Property(String name, BaseLiteral value) {
        this.name = name;
        valueLiteral = value;
    }

    public String getValue() {
        if (valueLiteral instanceof StringLiteral) {
            return ((StringLiteral)valueLiteral).getValue();
        }
        return valueLiteral.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) { return true; }
        if (o == null || getClass() != o.getClass()) { return false; }
        Property property = (Property)o;
        return StringUtils.equalsIgnoreCase(name, property.name) && Objects.equal(
            valueLiteral, property.valueLiteral);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(name, valueLiteral);
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitProperty(this, context);
    }
}
