package com.aliyun.fastmodel.transform.spark.context;

import lombok.Getter;

/**
 * table format
 *
 * @author panguanjing
 * @date 2023/2/18
 */
public enum SparkTableFormat {
    /**
     * https://spark.apache.org/docs/latest/sql-ref-syntax-ddl-create-table-datasource.html
     */
    DATASOURCE_FORMAT("dataSourceFormat"),

    /**
     * https://spark.apache.org/docs/latest/sql-ref-syntax-ddl-create-table-hiveformat.html
     */
    HIVE_FORMAT("hiveFormat");

    @Getter
    private final String value;

    SparkTableFormat(String value) {
        this.value = value;
    }

}
