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
import java.util.StringJoiner;
import java.util.stream.Collectors;

import com.aliyun.fastmodel.transform.api.client.dto.property.BaseClientProperty;
import com.aliyun.fastmodel.transform.hologres.client.property.DictEncodingColumn.ColumnStatus;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;

/**
 * DictEncodingColumns
 *
 * @author panguanjing
 * @date 2022/6/7
 */
public class DictEncodingColumn extends BaseClientProperty<List<ColumnStatus>> {

    /**
     * columns
     */
    public static final String DICTIONARY_ENCODING_COLUMN = "dictionary_encoding_columns";

    public enum Status {

        /**
         * on
         */
        ON("on"),
        /**
         * off
         */
        OFF("off"),
        /**
         * auto
         */
        AUTO("auto");

        @Getter
        private final String value;

        private Status(String value) {
            this.value = value;
        }

        public static Status getByValue(String value) {
            Status[] statuses = Status.values();
            for (Status status : statuses) {
                if (StringUtils.equalsIgnoreCase(status.getValue(), value)) {
                    return status;
                }
            }
            return Status.AUTO;
        }

    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ColumnStatus {
        /**
         * column name
         */
        private String columnName;
        /**
         * status
         */
        private Status status;
    }

    public DictEncodingColumn() {
        this.setKey(DICTIONARY_ENCODING_COLUMN);
    }

    @Override
    public String valueString() {
        List<String> list = this.getValue().stream().map(
            s -> new StringJoiner(":").add(s.getColumnName()).add(s.status.getValue()).toString()
        ).collect(Collectors.toList());
        return Joiner.on(",").join(list);
    }

    @Override
    public void setValueString(String value) {
        if (StringUtils.isBlank(value)) {
            this.setValue(Collections.emptyList());
            return;
        }
        List<String> list = Splitter.on(",").splitToList(value);
        List<ColumnStatus> columnStatuses = Lists.newArrayList();
        for (String s : list) {
            List<String> splitToList = Splitter.on(":").splitToList(s);
            ColumnStatus columnStatus = null;
            if (splitToList.size() > 1) {
                columnStatus = new ColumnStatus(
                    splitToList.get(0),
                    Status.getByValue(splitToList.get(1))
                );
            } else {
                columnStatus = new ColumnStatus(
                    splitToList.get(0),
                    Status.AUTO
                );
            }
            columnStatuses.add(columnStatus);
        }
        this.setValue(columnStatuses);
    }

}
