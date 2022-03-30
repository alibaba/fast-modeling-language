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

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/1/28
 */
public enum DialectName {

    /**
     * 模型引擎
     */
    FML,

    /**
     * MaxCompute引擎
     */
    MAXCOMPUTE,

    /**
     * Hologres引擎
     */
    HOLOGRES,

    /**
     * Hive引擎
     */
    HIVE,

    /**
     * PRESTO
     */
    PRESTO,

    /**
     * Spark引擎
     */
    SPARK,

    /**
     * IMPALA
     */
    IMPALA,

    /**
     * PlantUML
     */
    PLANTUML,

    /**
     * MYSQL
     */
    MYSQL,

    /**
     * Oracle
     */
    ORACLE,

    /**
     * Zen Code
     */
    ZEN,

    /**
     * 渲染图的结构
     */
    GRAPH;

    public static DialectName getByCode(String name) {
        DialectName[] dialectNames = DialectName.values();
        for (DialectName e : dialectNames) {
            if (e.name().equalsIgnoreCase(name)) {
                return e;
            }
        }
        throw new IllegalArgumentException("can't support the engine with:" + name);
    }
}
