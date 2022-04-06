/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.hologres.client.property;

import java.util.Collections;
import java.util.List;

import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

/**
 * 表的数据存储方式
 *
 * @author panguanjing
 * @date 2022/6/13
 */
public class TableOrientation extends BaseClientProperty<List<String>> {

    public static final String ORIENTATION = "orientation";

    public enum Orientation {
        /**
         * row
         */
        ROW("row"),
        /**
         * column
         */
        COLUMN("column");

        @Getter
        private final String value;

        Orientation(String value) {
            this.value = value;
        }
    }

    public TableOrientation() {
        this.setKey(ORIENTATION);
    }

    @Override
    public String valueString() {
        return Joiner.on(",").join(this.getValue());
    }

    @Override
    public void setValueString(String value) {
        if (StringUtils.isBlank(value)) {
            this.setValue(Collections.emptyList());
            return;
        }
        this.setValue(Splitter.on(",").splitToList(value));
    }
}
