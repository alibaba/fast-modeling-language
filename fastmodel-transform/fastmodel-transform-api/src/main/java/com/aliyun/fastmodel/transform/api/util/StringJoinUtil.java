/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.api.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.core.tree.QualifiedName;
import com.aliyun.fastmodel.core.tree.expr.Identifier;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;

/**
 * 字符join类，提供一些通用的join方式
 *
 * @author panguanjing
 * @date 2022/5/20
 */
public class StringJoinUtil {

    /**
     * 一个支持join的工具类，请确保第三个参数arg不能为空
     *
     * @param first  第一个
     * @param second 第二个
     * @param arg    实际的参数
     * @return {@link QualifiedName}
     * @throws AssertionError 如果arg是空
     */
    public static QualifiedName join(String... args) {
        if (args == null || args.length == 0) {
            return null;
        }
        if (StringUtils.isBlank(args[args.length - 1])) {
            ArrayList<Identifier> originalParts = Lists.newArrayList(new Identifier(args[args.length - 1]));
            return QualifiedName.of(originalParts);
        }
        List<Identifier> identifiers = Arrays.stream(args)
            .filter(StringUtils::isNotBlank)
            .map(Identifier::new).collect(Collectors.toList());
        return QualifiedName.of(identifiers);
    }
}
