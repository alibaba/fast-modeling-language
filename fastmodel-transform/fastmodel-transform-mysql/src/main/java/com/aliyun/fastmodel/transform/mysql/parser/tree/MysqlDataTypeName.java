package com.aliyun.fastmodel.transform.mysql.parser.tree;

import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName;
import com.aliyun.fastmodel.transform.api.datatype.simple.ISimpleDataTypeName;
import com.aliyun.fastmodel.transform.api.datatype.simple.SimpleDataTypeName;
import org.apache.commons.lang3.StringUtils;

/**
 * MySQL数据类型名称枚举
 *
 * @author panguanjing
 * @date 2025/10/11
 */
public enum MysqlDataTypeName implements ISimpleDataTypeName {

    /**
     * tinyint
     */
    TINYINT("TINYINT", Dimension.ONE, SimpleDataTypeName.NUMBER),

    /**
     * smallint
     */
    SMALLINT("SMALLINT", Dimension.ONE, SimpleDataTypeName.NUMBER),

    /**
     * mediumint
     */
    MEDIUMINT("MEDIUMINT", Dimension.ONE, SimpleDataTypeName.NUMBER),

    /**
     * int
     */
    INT("INT", Dimension.ONE, SimpleDataTypeName.NUMBER),

    /**
     * integer
     */
    INTEGER("INTEGER", Dimension.ONE, SimpleDataTypeName.NUMBER),

    /**
     * bigint
     */
    BIGINT("BIGINT", Dimension.ONE, SimpleDataTypeName.NUMBER),

    /**
     * float
     */
    FLOAT("FLOAT", Dimension.ONE, SimpleDataTypeName.NUMBER),

    /**
     * double
     */
    DOUBLE("DOUBLE", Dimension.TWO, SimpleDataTypeName.NUMBER),

    /**
     * decimal
     */
    DECIMAL("DECIMAL", Dimension.TWO, SimpleDataTypeName.NUMBER),

    /**
     * numeric
     */
    NUMERIC("NUMERIC", Dimension.TWO, SimpleDataTypeName.NUMBER),

    /**
     * bit
     */
    BIT("BIT", Dimension.ONE, SimpleDataTypeName.NUMBER),

    /**
     * boolean
     */
    BOOLEAN("BOOLEAN", Dimension.ZERO, SimpleDataTypeName.BOOLEAN),

    /**
     * date
     */
    DATE("DATE", Dimension.ZERO, SimpleDataTypeName.DATE),

    /**
     * datetime
     */
    DATETIME("DATETIME", Dimension.ZERO, SimpleDataTypeName.DATE),

    /**
     * timestamp
     */
    TIMESTAMP("TIMESTAMP", Dimension.ZERO, SimpleDataTypeName.DATE),

    /**
     * time
     */
    TIME("TIME", Dimension.ZERO, SimpleDataTypeName.DATE),

    /**
     * year
     */
    YEAR("YEAR", Dimension.ZERO, SimpleDataTypeName.DATE),

    /**
     * char
     */
    CHAR("CHAR", Dimension.ONE, SimpleDataTypeName.STRING),

    /**
     * varchar
     */
    VARCHAR("VARCHAR", Dimension.ONE, SimpleDataTypeName.STRING),

    /**
     * binary
     */
    BINARY("BINARY", Dimension.ONE, SimpleDataTypeName.STRING),

    /**
     * varbinary
     */
    VARBINARY("VARBINARY", Dimension.ONE, SimpleDataTypeName.STRING),

    /**
     * tinyblob
     */
    TINYBLOB("TINYBLOB", Dimension.ZERO, SimpleDataTypeName.STRING),

    /**
     * blob
     */
    BLOB("BLOB", Dimension.ZERO, SimpleDataTypeName.STRING),

    /**
     * mediumblob
     */
    MEDIUMBLOB("MEDIUMBLOB", Dimension.ZERO, SimpleDataTypeName.STRING),

    /**
     * longblob
     */
    LONGBLOB("LONGBLOB", Dimension.ZERO, SimpleDataTypeName.STRING),

    /**
     * tinytext
     */
    TINYTEXT("TINYTEXT", Dimension.ZERO, SimpleDataTypeName.STRING),

    /**
     * text
     */
    TEXT("TEXT", Dimension.ZERO, SimpleDataTypeName.STRING),

    /**
     * mediumtext
     */
    MEDIUMTEXT("MEDIUMTEXT", Dimension.ZERO, SimpleDataTypeName.STRING),

    /**
     * longtext
     */
    LONGTEXT("LONGTEXT", Dimension.ZERO, SimpleDataTypeName.STRING),

    /**
     * enum
     */
    ENUM("ENUM", Dimension.MULTIPLE, SimpleDataTypeName.STRING),

    /**
     * set
     */
    SET("SET", Dimension.MULTIPLE, SimpleDataTypeName.STRING),

    /**
     * json
     */
    JSON("JSON", Dimension.ZERO, SimpleDataTypeName.STRING),

    /**
     * geometry
     */
    GEOMETRY("GEOMETRY", Dimension.ZERO, SimpleDataTypeName.STRING),

    /**
     * point
     */
    POINT("POINT", Dimension.ZERO, SimpleDataTypeName.STRING),

    /**
     * linestring
     */
    LINESTRING("LINESTRING", Dimension.ZERO, SimpleDataTypeName.STRING),

    /**
     * polygon
     */
    POLYGON("POLYGON", Dimension.ZERO, SimpleDataTypeName.STRING),

    /**
     * multipoint
     */
    MULTIPOINT("MULTIPOINT", Dimension.ZERO, SimpleDataTypeName.STRING),

    /**
     * multilinestring
     */
    MULTILINESTRING("MULTILINESTRING", Dimension.ZERO, SimpleDataTypeName.STRING),

    /**
     * multipolygon
     */
    MULTIPOLYGON("MULTIPOLYGON", Dimension.ZERO, SimpleDataTypeName.STRING),

    /**
     * geometrycollection
     */
    GEOMETRYCOLLECTION("GEOMETRYCOLLECTION", Dimension.ZERO, SimpleDataTypeName.STRING);

    /**
     * multi prefix
     */
    public static final String MULTI_PREFIX = "<";

    private final String value;

    private final Dimension dimension;

    private final SimpleDataTypeName simpleDataTypeName;

    MysqlDataTypeName(String value, Dimension dimension,
        SimpleDataTypeName simpleDataTypeName) {
        this.value = value;
        this.dimension = dimension;
        this.simpleDataTypeName = simpleDataTypeName;
    }

    public static IDataTypeName getByValue(String value) {
        String v = value;
        if (v.indexOf(MULTI_PREFIX) > 0) {
            v = v.substring(0, v.indexOf(MULTI_PREFIX)).trim();
        }
        MysqlDataTypeName[] dataTypeNames = MysqlDataTypeName.values();
        for (MysqlDataTypeName s : dataTypeNames) {
            if (StringUtils.equalsIgnoreCase(s.getValue(), v)) {
                return s;
            }
        }
        throw new IllegalArgumentException("not support the dataType with value:" + value);
    }

    @Override
    public SimpleDataTypeName getSimpleDataTypeName() {
        return simpleDataTypeName;
    }

    @Override
    public String getName() {
        return name();
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public Dimension getDimension() {
        return dimension;
    }

}