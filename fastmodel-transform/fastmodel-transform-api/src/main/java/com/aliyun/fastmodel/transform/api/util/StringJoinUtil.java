/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.api.util;

import com.aliyun.fastmodel.core.tree.QualifiedName;
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
    public static QualifiedName join(String first, String second, String arg) {
        if (StringUtils.isBlank(arg)) {
            return QualifiedName.of(arg);
        }
        boolean firstNotBlank = StringUtils.isNotBlank(first);
        boolean secondNotBlank = StringUtils.isNotBlank(second);
        if (firstNotBlank && secondNotBlank) {
            return QualifiedName.of(first, second, arg);
        }
        if (firstNotBlank) {
            return QualifiedName.of(first, arg);
        }
        if (secondNotBlank) {
            return QualifiedName.of(second, arg);
        }
        return QualifiedName.of(arg);
    }
}
