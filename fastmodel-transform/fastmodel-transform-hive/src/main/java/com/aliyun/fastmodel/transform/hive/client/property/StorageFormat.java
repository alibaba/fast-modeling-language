/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.hive.client.property;

import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.aliyun.fastmodel.transform.hive.client.property.StorageFormat.StorageFormatEnum;
import com.aliyun.fastmodel.transform.hive.format.HivePropertyKey;
import org.apache.commons.lang3.StringUtils;

/**
 * https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-StorageFormatsStorageFormatsRowFormat,StorageFormat,andSerDe
 *
 * @author panguanjing
 * @date 2023/2/05
 */
public class StorageFormat extends BaseClientProperty<StorageFormatEnum> {

    public enum StorageFormatEnum {
        /**
         * text file
         */
        TEXTFILE("TEXTFILE"),
        /**
         * sequence file
         */
        SEQUENCEFILE("SEQUENCEFILE"),

        /**
         * orc
         */
        ORC("ORC"),

        /**
         * PARQUET
         */
        PARQUET("PARQUET"),

        /**
         * avro
         */
        AVRO("AVRO"),
        /**
         * rc file
         */
        RCFILE("RCFILE"),
        /**
         * json file
         */
        JSONFILE("JSONFILE");

        private final String value;

        StorageFormatEnum(String value) {
            this.value = value;
        }

        public static StorageFormatEnum getByValue(String value) {
            StorageFormatEnum[] storageFormatEnums = StorageFormatEnum.values();
            for (StorageFormatEnum storageFormatEnum : storageFormatEnums) {
                if (StringUtils.equalsIgnoreCase(storageFormatEnum.getValue(), value)) {
                    return storageFormatEnum;
                }
            }
            return null;
        }

        public String getValue() {
            return this.value;
        }
    }

    public StorageFormat() {
        this.setKey(HivePropertyKey.STORAGE_FORMAT.getValue());
    }

    @Override
    public String valueString() {
        return this.value.getValue();
    }

    @Override
    public void setValueString(String value) {
        this.value = StorageFormatEnum.getByValue(value);
    }

}
