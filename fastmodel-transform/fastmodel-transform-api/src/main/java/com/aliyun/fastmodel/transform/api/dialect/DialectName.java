/*
 * Copyright (c)  2021. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.api.dialect;

import lombok.Getter;

/**
 * 方言名字
 * 目前是按照引擎对应的方言进行处理。
 *
 * @author panguanjing
 * @date 2021/1/28
 */
public enum DialectName implements IDialectName {

    /**
     * 模型引擎
     */
    FML(Constants.FML),

    /**
     * MaxCompute引擎
     */
    MAXCOMPUTE(Constants.MAXCOMPUTE),

    /**
     * Hologres引擎
     */
    HOLOGRES(Constants.HOLOGRES),

    /**
     * Hive引擎
     */
    HIVE(Constants.HIVE),

    /**
     * PRESTO
     */
    PRESTO(Constants.PRESTO),

    /**
     * Spark引擎
     */
    SPARK(Constants.SPARK),

    /**
     * IMPALA
     */
    IMPALA(Constants.IMPALA),

    /**
     * PlantUML
     */
    PLANTUML(Constants.PLANTUML),

    /**
     * MYSQL
     */
    MYSQL(Constants.MYSQL),

    /**
     * Oracle
     */
    ORACLE(Constants.ORACLE),

    /**
     * Zen Code
     */
    ZEN(Constants.ZEN),

    /**
     * Flink
     */
    FLINK(Constants.FLINK),

    /**
     * adb
     */
    ADB_MYSQL(Constants.ADB_MYSQL),

    /**
     * adb pg
     */
    ADB_PG(Constants.ADB_PG),

    /**
     * postgresql
     */
    POSTGRESQL(Constants.POSTGRESQL),

    /**
     * json
     */
    JSON(Constants.JSON),

    /**
     * 渲染图的结构
     */
    GRAPH(Constants.GRAPH);
    @Getter
    private final String value;

    DialectName(String value) {this.value = value;}

    public static class Constants {
        public static final String FML = "FML";
        public static final String ODPS = "ODPS";
        public static final String MAXCOMPUTE = "MAX_COMPUTE";
        public static final String PLANTUML = "PLANTUML";
        public static final String MYSQL = "MYSQL";
        public static final String ORACLE = "ORACLE";
        public static final String ZEN = "ZEN";
        public static final String GRAPH = "GRAPH";
        public static final String IMPALA = "IMPALA";
        public static final String SPARK = "SPARK";
        public static final String PRESTO = "PRESTO";
        public static final String HIVE = "HIVE";
        public static final String HOLOGRES = "HOLOGRES";
        public static final String CLICKHOUSE = "CLICKHOUSE";
        public static final String FLINK = "FLINK";
        public static final String ADB_MYSQL = "ADB_MYSQL";
        public static final String ADB_PG = "ADB_PG";
        public static final String POSTGRESQL = "POSTGRESQL";

        public static final String JSON = "JSON";
    }

    public static DialectName getByCode(String name) {
        DialectName[] dialectNames = DialectName.values();
        if (Constants.ODPS.equalsIgnoreCase(name)) {
            return DialectName.MAXCOMPUTE;
        }
        //if name or value equal then return
        for (DialectName e : dialectNames) {
            if (e.getValue().equalsIgnoreCase(name) ||
                e.name().equalsIgnoreCase(name)) {
                return e;
            }
        }
        throw new IllegalArgumentException("can't support the engine with:" + name);
    }

    @Override
    public String getName() {
        return this.getValue();
    }
}
