/*
 * Copyright (c)  2020. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.core.formatter;

import com.aliyun.fastmodel.core.tree.Node;

/**
 * FastModelFormatter
 *
 * @author panguanjing
 * @date 2020/11/23
 */
public class FastModelFormatter {

    public static String formatNode(Node root) {
        if (root == null) {
            throw new IllegalArgumentException("root can't be null");
        }
        FastModelVisitor fastModelVisitor = new FastModelVisitor();
        fastModelVisitor.process(root, 0);
        return fastModelVisitor.getBuilder().toString();
    }

}
