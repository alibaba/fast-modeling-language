/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.core.tree.datatype;

/**
 * 数据类型
 *
 * @author panguanjing
 * @date 2022/6/6
 */
public interface IDataTypeName {

    enum Dimension {
        /**
         * 无维度
         */
        ZERO,
        /**
         * one
         */
        ONE,
        /**
         * two
         */
        TWO,
        /**
         * 多维度
         */
        MULTIPLE;
    }

    /**
     * 数据类型的名字
     *
     * @return 名称
     */
    String getName();

    /**
     * 实际的值
     *
     * @return 具体的值
     */
    String getValue();

    /**
     * 默认维度设置
     *
     * @return {@link Dimension}
     */
    default Dimension getDimension() {
        return Dimension.ZERO;
    }

}
