package com.aliyun.fastmodel.transform.starrocks.parser.tree.datatype;

import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName;
import com.aliyun.fastmodel.transform.api.datatype.simple.ISimpleDataTypeName;
import com.aliyun.fastmodel.transform.api.datatype.simple.SimpleDataTypeName;
import org.apache.commons.lang3.StringUtils;

/**
 * starRocks DataType Name
 *
 * @author panguanjing
 * @date 2023/9/12
 */
public enum StarRocksDataTypeName implements ISimpleDataTypeName {
    /**
     * boolean
     */
    BOOLEAN("BOOLEAN", Dimension.ZERO, SimpleDataTypeName.BOOLEAN),

    /**
     * tinyint
     */
    TINYINT("TINYINT", Dimension.ONE, SimpleDataTypeName.NUMBER),

    /**
     * small int
     */
    SMALLINT("SMALLINT", Dimension.ONE, SimpleDataTypeName.NUMBER),

    /**
     * signed int
     */
    SIGNED_INT("SIGNED INT", Dimension.ZERO, SimpleDataTypeName.NUMBER),

    /**
     * singed integer
     */
    SIGNED_INTEGER("SIGNED INTEGER", Dimension.ZERO, SimpleDataTypeName.NUMBER),

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
     * large int
     */
    LARGEINT("LARGEINT", Dimension.ONE, SimpleDataTypeName.NUMBER),

    /**
     * float
     */
    FLOAT("FLOAT", Dimension.ZERO, SimpleDataTypeName.NUMBER),

    /**
     * double
     */
    DOUBLE("DOUBLE", Dimension.ZERO, SimpleDataTypeName.NUMBER),

    /**
     * date
     */
    DATE("DATE", Dimension.ZERO, SimpleDataTypeName.DATE),

    /**
     * datetime
     */
    DATETIME("DATETIME", Dimension.ZERO, SimpleDataTypeName.DATE),

    /**
     * time
     */
    TIME("TIME", Dimension.ZERO, SimpleDataTypeName.DATE),

    /**
     * char
     */
    CHAR("CHAR", Dimension.ONE, SimpleDataTypeName.STRING),

    /**
     * varchar
     */
    VARCHAR("VARCHAR", Dimension.ONE, SimpleDataTypeName.STRING),

    /**
     * string
     */
    STRING("STRING", Dimension.ZERO, SimpleDataTypeName.STRING),

    /**
     * text
     */
    TEXT("TEXT", Dimension.ZERO, SimpleDataTypeName.STRING),

    /**
     * bitmap
     */
    BITMAP("BITMAP", Dimension.ZERO, SimpleDataTypeName.NUMBER),

    /**
     * hll
     */
    HLL("HLL", Dimension.ZERO, SimpleDataTypeName.NUMBER),

    /**
     * percentile
     */
    PERCENTILE("PERCENTILE", Dimension.ZERO, SimpleDataTypeName.NUMBER),

    /**
     * json
     */
    JSON("JSON", Dimension.ZERO, SimpleDataTypeName.STRING),

    /**
     * varbinary
     */
    VARBINARY("VARBINARY", Dimension.ONE, SimpleDataTypeName.STRING),

    /**
     * binary
     */
    BINARY("BINARY", Dimension.ONE, SimpleDataTypeName.STRING),

    /**
     * decimal
     */
    DECIMAL("DECIMAL", Dimension.TWO, SimpleDataTypeName.NUMBER),

    /**
     * decimal
     */
    DECIMALV2("DECIMALV2", Dimension.TWO, SimpleDataTypeName.NUMBER),

    /**
     * decimal32
     */
    DECIMAL32("DECIMAL32", Dimension.TWO, SimpleDataTypeName.NUMBER),

    /**
     * decimal64
     */
    DECIMAL64("DECIMAL64", Dimension.TWO, SimpleDataTypeName.NUMBER),

    /**
     * decimal128
     */
    DECIMAL128("DECIMAL128", Dimension.TWO, SimpleDataTypeName.NUMBER),

    /**
     * numeric
     */
    NUMERIC("NUMERIC", Dimension.TWO, SimpleDataTypeName.NUMBER),

    /**
     * number
     */
    NUMBER("NUMBER", Dimension.TWO, SimpleDataTypeName.NUMBER),

    /**
     * Array
     */
    ARRAY("ARRAY", Dimension.MULTIPLE, SimpleDataTypeName.STRING),

    /**
     * MAP
     */
    Map("MAP", Dimension.MULTIPLE, SimpleDataTypeName.STRING),
    ;

    /**
     * multi prefix
     */
    public static final String MULTI_PREFIX = "<";

    private final String value;

    private final Dimension dimension;

    private final SimpleDataTypeName simpleDataTypeName;

    StarRocksDataTypeName(String value, Dimension dimension,
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
        StarRocksDataTypeName[] starRocksDataTypeNames = StarRocksDataTypeName.values();
        for (StarRocksDataTypeName s : starRocksDataTypeNames) {
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
