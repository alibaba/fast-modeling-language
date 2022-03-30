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
import java.util.stream.Collectors;

import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import lombok.EqualsAndHashCode;
import lombok.Getter;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Locale.ENGLISH;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2020/11/3
 */
@Getter
@EqualsAndHashCode(callSuper = false)
public class QualifiedName extends AbstractNode {

    private final List<String> parts;

    private final List<Identifier> originalParts;

    private QualifiedName(List<Identifier> originalParts) {
        this.originalParts = originalParts;
        parts = originalParts.stream().map(identifier -> identifier.getValue().toLowerCase(ENGLISH)).collect(
            toImmutableList());
    }

    public static QualifiedName of(String name) {
        Preconditions.checkNotNull(name);
        Splitter splitter = Splitter.on(".");
        List<String> strings = splitter.splitToList(name);
        return new QualifiedName(strings.stream().map(Identifier::new).collect(Collectors.toList()));
    }

    public static QualifiedName of(String first, String... rest) {
        return of(ImmutableList.copyOf(Lists.asList(first, rest).stream().map(Identifier::new)
            .collect(Collectors.toList())));
    }

    public static QualifiedName of(Iterable<Identifier> originalParts) {
        return new QualifiedName(ImmutableList.copyOf(originalParts));
    }

    public String getFirst() {
        return Iterables.getFirst(parts, null);
    }

    public String getSuffix() {
        return Iterables.getLast(parts);
    }

    /**
     * 只有大小超过1个的情况下，才返回值
     * a.b => a
     * b => null
     * a => null
     *
     * @return
     */
    public String getFirstIfSizeOverOne() {
        if (parts.size() <= 1) {
            return null;
        }
        return getFirst();
    }

    /**
     * a.b => a
     * b => null
     *
     * @return
     */
    public Identifier getFirstIdentifierIfSizeOverOne() {
        if (parts.size() <= 1) {
            return null;
        }
        return new Identifier(getFirst());
    }

    /**
     * 获取后缀的路径
     * a.b.c ===>   b.c
     * a ===> a
     *
     * @return
     */
    public String getSuffixPath() {
        String firstIfSizeOverOne = getFirstIfSizeOverOne();
        if (firstIfSizeOverOne == null) {
            return getSuffix();
        }
        return Joiner.on(".").join(getParts().subList(1, getParts().size()));
    }

    /**
     * 获取前置路径
     * a.b.c => a.b
     * c => null
     *
     * @return
     */
    public String getPrefixPath() {
        if (getParts().size() <= 1) {
            return null;
        }
        return Joiner.on(".").join(getParts().subList(0, getParts().size() - 1));
    }

    @Override

    public String toString() {
        return Joiner.on('.').join(parts);
    }

    @Override
    public List<? extends Node> getChildren() {
        ImmutableList.Builder<Node> builder = new Builder<>();
        builder.addAll(originalParts);
        return builder.build();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitQualifiedName(this, context);
    }
}