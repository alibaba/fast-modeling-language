/*
 * Copyright (c)  2021. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.core.parser;

import com.aliyun.fastmodel.core.exception.ParseException;
import com.aliyun.fastmodel.core.tree.Node;
import com.aliyun.fastmodel.core.tree.datatype.BaseDataType;

/**
 * 一个通用的语言parser
 *
 * @author panguanjing
 * @date 2021/7/15
 */
public interface LanguageParser<T extends Node, C> {

    /**
     * parse text to node
     *
     * @param text
     * @param <T>
     * @return
     * @throws ParseException
     */
    default <T> T parseNode(String text) throws ParseException {
        return parseNode(text, null);
    }

    /**
     * parse text to node with context
     *
     * @param text
     * @param context
     * @param <T>
     * @return
     * @throws ParseException
     */
    <T> T parseNode(String text, C context) throws ParseException;

    /**
     * parse data type
     *
     * @param code
     * @param context
     * @return {@link BaseDataType}
     * @throws ParseException
     */
    default BaseDataType parseDataType(String code, C context) throws ParseException {
        return null;
    }
}
