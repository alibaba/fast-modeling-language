/*
 * Copyright [2024] [name of copyright owner]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aliyun.fastmodel.transform.adbpg.format;

import com.aliyun.fastmodel.transform.api.format.PropertyKey;
import com.aliyun.fastmodel.transform.api.format.PropertyValueType;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

/**
 * @author 云异
 * @date 2025/2/14
 */
@Getter
public enum AdbPgPropertyKey implements PropertyKey {

    /**
     * 只更新
     */
    APPEND_ONLY("appendonly", true),

    /**
     * 存储格式
     */
    ORIENTATION("orientation", true),

    /**
     * 压缩类型
     */
    COMPRESSTYPE("compresstype", true),

    /**
     * 压缩级别
     */
    COMPRESSLEVEL("compresslevel", true);

    private final String value;

    private final boolean supportPrint;

    private final PropertyValueType valueType;

    AdbPgPropertyKey(String value) {
        this(value, false, PropertyValueType.STRING_LITERAL);
    }

    AdbPgPropertyKey(String value, boolean supportPrint) {
        this(value, supportPrint, PropertyValueType.STRING_LITERAL);
    }

    AdbPgPropertyKey(String value, boolean supportPrint, PropertyValueType valueType) {
        this.value = value;
        this.supportPrint = supportPrint;
        this.valueType = valueType;
    }

    public static AdbPgPropertyKey getByValue(String value) {
        AdbPgPropertyKey[] values = AdbPgPropertyKey.values();
        for (AdbPgPropertyKey adbPgPropertyKey : values) {
            if (StringUtils.equalsIgnoreCase(adbPgPropertyKey.getValue(), value)) {
                return adbPgPropertyKey;
            }
        }
        return null;
    }
}
