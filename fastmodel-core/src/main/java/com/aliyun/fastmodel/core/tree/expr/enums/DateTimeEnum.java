/*
 * Copyright 2021-2022 Alibaba Group Holding Ltd.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.aliyun.fastmodel.core.tree.expr.enums;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2020/9/23
 */
public enum DateTimeEnum {
    /**
     * 年
     */
    YEAR,
    /**
     * 月
     */
    MONTH,

    /**
     * 星期
     */
    WEEK,
    /**
     * 日
     */
    DAY,
    /**
     * 小时
     */
    HOUR,
    /**
     * 刻钟
     */
    QUARTER,
    /**
     * 分钟
     */
    MINUTE,
    /**
     * 秒
     */
    SECOND;


    public static DateTimeEnum getByCode(String code) {
        DateTimeEnum[] dateTimeEnums = DateTimeEnum.values();
        for (DateTimeEnum dateTimeEnum : dateTimeEnums) {
            if (dateTimeEnum.name().equalsIgnoreCase(code)) {
                return dateTimeEnum;
            }
        }
        throw new IllegalArgumentException("can't find the dateTime with code:" + code);
    }
}
