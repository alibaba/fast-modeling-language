/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.compare.impl.helper;

import java.util.Optional;

import com.aliyun.fastmodel.core.tree.Node;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * 比对的内容对象
 *
 * @author panguanjing
 * @date 2021/11/3
 */
@Getter
@ToString
@EqualsAndHashCode
public class ComparePair {
    private final Optional<Node> left;
    private final Optional<Node> right;

    public ComparePair(Optional<Node> left, Optional<Node> right) {
        this.left = left;
        this.right = right;
    }
}
