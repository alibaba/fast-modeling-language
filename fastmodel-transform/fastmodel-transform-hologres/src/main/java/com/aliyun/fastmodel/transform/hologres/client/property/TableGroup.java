/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.hologres.client.property;

import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;

/**
 * table group
 *
 * @author panguanjing
 * @date 2022/6/13
 */
public class TableGroup extends BaseClientProperty<String> {

    public static final String TABLE_GROUP = HoloPropertyKey.TABLE_GROUP.getValue();

    public TableGroup() {
        this.setKey(TABLE_GROUP);
    }

    @Override
    public String valueString() {
        return this.getValue();
    }

    @Override
    public void setValueString(String value) {
        this.setValue(value);
    }
}
