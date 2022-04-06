/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.hologres.parser.util;

import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

import lombok.SneakyThrows;

/**
 * builder util
 *
 * @author panguanjing
 * @date 2022/6/16
 */
public class BuilderUtil {
    @SneakyThrows
    public static void addTransaction(Appendable appendable, Supplier<String> predicate) {
        appendable.append("BEGIN;\n");
        appendable.append(predicate.get());
        appendable.append("\nCOMMIT;");
    }
}
