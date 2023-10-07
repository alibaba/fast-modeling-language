/*
 * Copyright (c)  2020. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
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
     * maxcompute
     */
    public static final DialectMeta DEFAULT_MAX_COMPUTE = createDefault(DialectName.MAXCOMPUTE);
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
     * 引擎的名字，唯一标示，大小写不敏感
     */
    private final IDialectName dialectName;

    /**
     * 引擎的版本，不同的版本，对应生成的Transform可能会存在差异
     */
    private final IVersion version;

    /**
     * Create Default
     *
     * @param maxcompute
     * @return {@link DialectMeta}
     */
    public static DialectMeta createDefault(IDialectName maxcompute) {
        return new DialectMeta(maxcompute, IVersion.getDefault());
    }

    public DialectMeta(IDialectName dialectName, IVersion version) {
        this.dialectName = dialectName;
        this.version = version;
    }

    public static DialectMeta getMaxCompute() {
        return DEFAULT_MAX_COMPUTE;
    }

    public static DialectMeta getHive() {
        return DEFAULT_HIVE;
    }

    public static DialectMeta getHologres() {
        return DEFAULT_HOLO;
    }

    public static DialectMeta getByNameAndVersion(String name, IVersion version) {
        return new DialectMeta(DialectName.getByCode(name), version);
    }

    /**
     * Get By Name
     *
     * @param dialectName
     * @return {@link DialectMeta}
     */
    public static DialectMeta getByName(IDialectName dialectName) {
        return new DialectMeta(dialectName, IVersion.getDefault());
    }

    @Override
    public String toString() {
        if (version == null || StringUtils.isBlank(version.getName())) {
            return this.getDialectName().getName();
        } else {
            return this.getDialectName().getName() + getVersion().getName();
        }
    }

}
