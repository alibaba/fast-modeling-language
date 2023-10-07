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

package com.aliyun.fastmodel.core.tree.util;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;

/**
 * identifier util
 *
 * @author panguanjing
 * @date 2020/9/10
 */
public class IdentifierUtil {

    public static final String SYS = "SYS_";
    public static final String DELIMITER = "`";

    /**
     * 生成系统的identifier
     *
     * @return 系统的标示
     */
    public static Identifier sysIdentifier() {
        return new Identifier(SYS + UUID.randomUUID().toString().replaceAll("-", ""));
    }

    public static Identifier sysPrimaryConstraintName(List<Identifier> colNames) {
        String newConstraintName = SYS + "(" + colNames.stream().map(Identifier::getValue).collect(
            Collectors.joining(",")) + ")";
        return new Identifier(newConstraintName);
    }

    public static List<Identifier> extractColumns(Identifier primaryConstraintName) {
        if (primaryConstraintName == null || !primaryConstraintName.getValue().startsWith(SYS)) {
            return ImmutableList.of();
        }
        String substring = primaryConstraintName.getValue().substring(SYS.length());
        if (substring.isEmpty()) {
            return ImmutableList.of();
        }
        int start = substring.indexOf("(");
        int end = substring.indexOf(")");
        if (start == -1 || end == -1) {
            return ImmutableList.of();
        }
        return Lists.newArrayList(substring.substring(start + 1, end).split(",")).stream().map(x -> {
            return new Identifier(x);
        }).collect(Collectors.toList());
    }

    /**
     * 将标识进行转义处理
     *
     * @param value
     * @return
     */
    public static String delimit(String value) {
        if (StringUtils.isBlank(value)) {
            return value;
        }
        Identifier identifier = new Identifier(value);
        if (identifier.isDelimited()) {
            return identifier.getValue();
        }
        return DELIMITER + identifier.getValue() + DELIMITER;
    }

    /**
     * 是否系统的标识
     *
     * @param identifier
     * @return
     */
    public static boolean isSysIdentifier(Identifier identifier) {
        return identifier != null && identifier.getValue().toUpperCase().startsWith(SYS);
    }
}
