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

package com.aliyun.fastmodel.core.tree.expr;

import java.util.List;
import java.util.regex.Pattern;

import com.aliyun.fastmodel.core.tree.IAstVisitor;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.NodeLocation;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

/**
 * 标识符，列名、表名等等
 *
 * @author panguanjing
 * @date 2020/10/30
 */
@Getter
public class Identifier extends BaseExpression {

    private final String value;
    private final boolean delimited;

    private static final Pattern NAME_PATTERN = Pattern.compile("[a-zA-Z0-9]([a-zA-Z0-9_:@])*");
    private static final Pattern DIGIT_PATTERN = Pattern.compile("[0-9]*");

    public Identifier(NodeLocation location, String origin, String value) {
        super(location, origin);
        this.value = value;
        delimited = !NAME_PATTERN.matcher(value).matches() || DIGIT_PATTERN.matcher(value).matches();
    }

    public Identifier(String value) {
        this(null, null, value);
    }

    public Identifier(String value, boolean delimited) {
        this(null, null, value, delimited);
    }

    public Identifier(NodeLocation location, String origin, String value, boolean delimited) {
        super(location, origin);
        this.value = value;
        this.delimited = delimited;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {return true;}
        if (o == null || getClass() != o.getClass()) {return false;}
        Identifier that = (Identifier)o;
        //只看value的值
        return StringUtils.equalsIgnoreCase(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(value);
    }

    @Override
    public List<? extends Node> getChildren() {
        return ImmutableList.of();
    }

    @Override
    public <R, C> R accept(IAstVisitor<R, C> visitor, C context) {
        return visitor.visitIdentifier(this, context);
    }

}
