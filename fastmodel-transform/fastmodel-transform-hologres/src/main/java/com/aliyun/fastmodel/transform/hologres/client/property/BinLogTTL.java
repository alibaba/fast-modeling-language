/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.hologres.client.property;

import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import org.apache.commons.lang3.StringUtils;

/**
 * CALL SET_TABLE_PROPERTY('molin_db.test_create_table', 'binlog.ttl', '86400');
 * <a href="https://help.aliyun.com/document_detail/201024.htm?spm=a2c4g.11186623.0.0.4e054ad5aLMB8E#concept-2037122">...</a>
 *
 * @author panguanjing
 * @date 2022/6/20
 */
public class BinLogTTL extends BaseClientProperty<Long> {

    public static final String BINLOG_TTL = HoloPropertyKey.BINLOG_TTL.getValue();

    public BinLogTTL() {
        this.setKey(HoloPropertyKey.BINLOG_TTL.getValue());
    }

    @Override
    public String valueString() {
        return String.valueOf(this.getValue());
    }

    @Override
    public void setValueString(String value) {
        if (StringUtils.isBlank(value)) {
            this.setValue(null);
        } else {
            this.setValue(Long.valueOf(value));
        }
    }
}
