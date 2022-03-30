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

package com.aliyun.fastmodel.transform.api.dialect;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;

/**
 * 引擎元数据
 *
 * @author panguanjing
 * @date 2020/10/16
 */

@Getter
@Setter
@EqualsAndHashCode(callSuper = false)
public class DialectMeta {

    /**
     * 不传的默认版本
     */
    public static final String DEFAULT_VERSION = "";
    /**
     * 引擎的名字，唯一标示，大小写不敏感
     */
    private final DialectName name;

    /**
     * 引擎的版本，不同的版本，对应生成的Transform可能会存在差异
     */
    private final String version;

    /**
     * hive
     */
    public static final DialectMeta DEFAULT_HIVE = createDefault(DialectName.HIVE);
    /**
     * holo
     */
    public static final DialectMeta DEFAULT_HOLO = createDefault(DialectName.HOLOGRES);

    /**
     * mysql
     */
    public static final DialectMeta DEFAULT_MYSQL = createDefault(DialectName.MYSQL);

    /**
     * Create Default
     *
     * @param maxcompute
     * @return {@link DialectMeta}
     */
    public static DialectMeta createDefault(DialectName maxcompute) {
        return new DialectMeta(maxcompute, DEFAULT_VERSION);
    }

    public DialectMeta(DialectName name, String version) {
        this.name = name;
        this.version = version;
    }

    public static DialectMeta getHive() {
        return DEFAULT_HIVE;
    }

    public static DialectMeta getHologres() {
        return DEFAULT_HOLO;
    }

    public static DialectMeta getByNameAndVersion(String name, String version) {
        return new DialectMeta(DialectName.getByCode(name), version);
    }

    /**
     * Get By Name
     *
     * @param dialectName
     * @return {@link DialectMeta}
     */
    public static DialectMeta getByName(DialectName dialectName) {
        return new DialectMeta(dialectName, DEFAULT_VERSION);
    }

    @Override
    public String toString() {
        if (StringUtils.isBlank(getVersion())) {
            return getName().name();
        } else {
            return getName().name() + getVersion();
        }
    }

}
