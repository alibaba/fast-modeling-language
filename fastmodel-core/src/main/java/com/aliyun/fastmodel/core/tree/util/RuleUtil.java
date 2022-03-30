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

import java.util.stream.Collectors;

import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.aliyun.fastmodel.core.tree.statement.rule.function.BaseFunctionName;
import com.aliyun.fastmodel.core.tree.statement.rule.function.FunctionGrade;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;

/**
 * 规则相关工具类
 *
 * @author panguanjing
 * @date 2021/5/31
 */
public class RuleUtil {

    public static final String PREFIX = "sys_rules_";

    public static boolean isSysGenerate(QualifiedName name) {
        return name.getSuffix().startsWith(PREFIX);
    }

    /**
     * 根据来源生成指定的规则名
     *
     * @param source
     * @return
     */
    public static QualifiedName generateRulesName(QualifiedName source) {
        if (source == null) {
            return null;
        }
        String suffix = source.getSuffix();
        return QualifiedName.of(PREFIX + suffix);
    }

    public static Identifier generateRuleNameByFunction(BaseFunctionName functionName, Identifier... arguments) {
        FunctionGrade functionGrade = functionName.getFunctionGrade();
        String description = functionName.getDescription();
        if (StringUtils.isNotBlank(functionName.getAliasName())) {
            description = functionName.getAliasName();
        }
        String joinText = Lists.newArrayList(arguments).stream().map(Identifier::getValue).collect(
            Collectors.joining(","));
        String r = String.format("%s-%s-(%s)", functionGrade.getDescription(), description,
            joinText);
        return new Identifier(r);
    }

    /**
     * 替换rule规则内容
     *
     * @param oldRuleName
     * @param pattern
     * @return
     */
    public static String replaceRuleName(String oldRuleName, String find, String replace) {
        if (oldRuleName == null) {
            return null;
        }
        if (replace == null) {
            return oldRuleName;
        }
        return StringUtils.replace(oldRuleName, find, replace);
    }

}
