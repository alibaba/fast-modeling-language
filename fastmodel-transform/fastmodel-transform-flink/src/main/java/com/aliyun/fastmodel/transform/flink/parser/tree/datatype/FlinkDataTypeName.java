package com.aliyun.fastmodel.transform.flink.parser.tree.datatype;

import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName;
import com.aliyun.fastmodel.transform.api.datatype.simple.ISimpleDataTypeName;
import com.aliyun.fastmodel.transform.api.datatype.simple.SimpleDataTypeName;
import org.apache.commons.lang3.StringUtils;

/**
 * @author 子梁
 * @date 2024/5/15
 */
public enum FlinkDataTypeName implements ISimpleDataTypeName {

    /**
     * DATE
     */
    DATE("DATE", Dimension.ZERO, SimpleDataTypeName.DATE),

    BOOLEAN("BOOLEAN", Dimension.ZERO, SimpleDataTypeName.BOOLEAN),

    CHAR("CHAR", Dimension.ONE, SimpleDataTypeName.STRING),

    VARCHAR("VARCHAR", Dimension.ONE, SimpleDataTypeName.STRING),

    STRING("STRING", Dimension.ONE, SimpleDataTypeName.STRING),

    BINARY("BINARY", Dimension.ONE, SimpleDataTypeName.STRING),

    VARBINARY("VARBINARY", Dimension.ONE, SimpleDataTypeName.STRING),

    BYTES("BYTES", Dimension.ONE, SimpleDataTypeName.STRING),

    TINYINT("TINYINT", Dimension.ONE, SimpleDataTypeName.NUMBER),

    SMALLINT("SMALLINT", Dimension.ONE, SimpleDataTypeName.NUMBER),

    INTEGER("INTEGER", Dimension.ONE, SimpleDataTypeName.NUMBER),

    BIGINT("BIGINT", Dimension.ONE, SimpleDataTypeName.NUMBER),

    FLOAT("FLOAT", Dimension.TWO, SimpleDataTypeName.NUMBER),

    DOUBLE("DOUBLE", Dimension.TWO, SimpleDataTypeName.NUMBER),

    TIME("TIME", Dimension.ONE, SimpleDataTypeName.DATE),

    TIMESTAMP("TIMESTAMP", Dimension.ONE, SimpleDataTypeName.DATE),

    TIMESTAMP_LTZ("TIMESTAMP_LTZ", Dimension.ONE, SimpleDataTypeName.DATE),

    TIMESTAMP_WITH_LOCAL_TIME_ZONE("TIMESTAMP_WITH_LOCAL_TIME_ZONE", Dimension.ONE, SimpleDataTypeName.DATE),

    INTERVAL("INTERVAL", Dimension.ONE, SimpleDataTypeName.DATE),

    ARRAY("ARRAY", Dimension.MULTIPLE, SimpleDataTypeName.STRING),

    MULTISET("MULTISET", Dimension.ONE, SimpleDataTypeName.STRING),

    MAP("MAP", Dimension.MULTIPLE, SimpleDataTypeName.STRING),

    ROW("ROW", Dimension.MULTIPLE, SimpleDataTypeName.STRING),

    RAW("RAW", Dimension.MULTIPLE, SimpleDataTypeName.STRING),

    DECIMAL("DECIMAL", Dimension.TWO, SimpleDataTypeName.NUMBER),

    ;


    /**
     * multi prefix
     */
    public static final String MULTI_PREFIX = "<";

    public static final String MULTI_PREFIX_BRACKET = "(";

    private final String value;

    private final Dimension dimension;

    private final SimpleDataTypeName simpleDataTypeName;

    FlinkDataTypeName(String value, Dimension dimension,
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
        if (v.indexOf(MULTI_PREFIX_BRACKET) > 0) {
            v = v.substring(0, v.indexOf(MULTI_PREFIX_BRACKET)).trim();
        }
        FlinkDataTypeName[] dataTypeNames = FlinkDataTypeName.values();
        for (FlinkDataTypeName s : dataTypeNames) {
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
