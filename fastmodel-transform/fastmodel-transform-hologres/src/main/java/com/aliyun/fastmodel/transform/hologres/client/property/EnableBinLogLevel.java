/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.hologres.client.property;

import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.aliyun.fastmodel.transform.hologres.client.property.EnableBinLogLevel.BinLogLevel;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

/**
 * 是否开启binlog
 * CALL SET_TABLE_PROPERTY('molin_db.test_create_table', 'binlog.level', 'replica');
 *
 * @author panguanjing
 * @date 2022/6/20
 */
public class EnableBinLogLevel extends BaseClientProperty<BinLogLevel> {

    @Getter
    public enum BinLogLevel {
        /**
         * replica
         */
        REPLICA("replica"),
        /**
         * none
         */
        NONE("none");

        private final String value;

        BinLogLevel(String value) {this.value = value;}

        public static BinLogLevel getByValue(String value) {
            BinLogLevel[] bingLogLevels = BinLogLevel.values();
            for (BinLogLevel b : bingLogLevels) {
                if (StringUtils.equalsIgnoreCase(b.getValue(), value)) {
                    return b;
                }
            }
            return BinLogLevel.NONE;
        }

    }

    public static final String ENABLE_BINLOG = "binlog.level";

    public EnableBinLogLevel() {
        this.setKey(ENABLE_BINLOG);
    }

    @Override
    public String valueString() {
        if (this.getValue() == null) {
            return BinLogLevel.NONE.getValue();
        }
        return String.valueOf(this.getValue().getValue());
    }

    @Override
    public void setValueString(String value) {
        if (StringUtils.isBlank(value)) {
            this.setValue(BinLogLevel.NONE);
        } else {
            this.setValue(BinLogLevel.getByValue(value));
        }
    }
}
