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

package com.aliyun.fastmodel.transform.adbmysql.parser.tree.index;

import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

/**
 * index option name
 *
 * @author panguanjing
 * @date 2024/10/8
 */
public enum IndexOptionName {
    /**
     * index type, 全文索引还是向量索引
     */
    INDEX_TYPE("index_type"),

    /**
     * index comment
     */
    INDEX_COMMENT("index_comment"),

    /**
     * analyzer
     */
    ANALYZER("analyzer"),

    /**
     * parser
     */
    PARSER("parser"),
    /**
     * algorithm
     */
    ALGORITHM("algorithm"),

    /**
     * DISTANCEMEASURE
     */
    DISTANCEMEASURE("distancemeasure"),

    ;
    @Getter
    private final String value;

    IndexOptionName(String value) {
        this.value = value;
    }

    /**
     * from value
     *
     * @param value
     * @return
     */
    public static IndexOptionName fromValue(String value) {
        IndexOptionName[] indexOptionNames = IndexOptionName.values();
        for (IndexOptionName indexOptionName : indexOptionNames) {
            if (StringUtils.equalsIgnoreCase(indexOptionName.value, value)) {
                return indexOptionName;
            }
        }
        return null;
    }
}
