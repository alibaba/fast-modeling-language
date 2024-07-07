/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.api.client.dto.property;

import java.util.List;

import com.google.common.collect.ImmutableList;
import lombok.Data;

/**
 * 属性
 *
 * @author panguanjing
 * @date 2022/6/6
 */
@Data
public abstract class BaseClientProperty<T> {
    /**
     * key
     */
    protected String key;

    /**
     * value
     */
    protected T value;

    /**
     * 将value作为string展示
     *
     * @return
     */
    public abstract String valueString();

    /**
     * 根据string获取value
     *
     * @param value
     * @return
     */
    public abstract void setValueString(String value);

    /**
     * 是否columnList properties
     *
     * @return
     */
    public List<String> toColumnList() {
        return ImmutableList.of();
    }

    /**
     * 设置column
     *
     * @param columnList
     */
    public void setColumnList(List<String> columnList) {
    }
}
