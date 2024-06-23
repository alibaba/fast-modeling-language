package com.aliyun.fastmodel.transform.oceanbase.parser.tree;

import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName;
import com.aliyun.fastmodel.transform.api.datatype.simple.ISimpleDataTypeName;
import com.aliyun.fastmodel.transform.api.datatype.simple.SimpleDataTypeName;
import org.apache.commons.lang3.StringUtils;

/**
 * OceanBaseMysqlDataTypeName
 *
 * @author panguanjing
 * @date 2024/2/2
 */
public enum OceanBaseMysqlDataTypeName implements ISimpleDataTypeName {

    TINYINT("TINYINT", Dimension.ZERO, SimpleDataTypeName.NUMBER),
    INT("INT", Dimension.ZERO, SimpleDataTypeName.NUMBER),
    BIGINT("BIGINT", Dimension.ZERO, SimpleDataTypeName.NUMBER),
    MEDIUMINT("MEDIUMINT", Dimension.ZERO, SimpleDataTypeName.NUMBER),
    BOOL("BOOL", Dimension.ZERO, SimpleDataTypeName.BOOLEAN),
    BOOLEAN("BOOLEAN", Dimension.ZERO, SimpleDataTypeName.BOOLEAN),
    INTEGER("INTEGER", Dimension.ZERO, SimpleDataTypeName.NUMBER),
    SMALLINT("SMALLINT", Dimension.ZERO, SimpleDataTypeName.NUMBER),
    FLOAT("FLOAT", Dimension.ZERO, SimpleDataTypeName.NUMBER),
    DOUBLE("DOUBLE", Dimension.ZERO, SimpleDataTypeName.NUMBER),
    BIT("BIT", Dimension.ZERO, SimpleDataTypeName.NUMBER),
    DOUBLE_PRECISION("DOUBLE PRECISION", Dimension.ZERO, SimpleDataTypeName.NUMBER),
    DECIMAL("DECIMAL", Dimension.TWO, SimpleDataTypeName.NUMBER),
    NUMBER("NUMBER", Dimension.TWO, SimpleDataTypeName.NUMBER),
    FIXED("FIXED", Dimension.TWO, SimpleDataTypeName.NUMBER),
    DATETIME("DATETIME", Dimension.ZERO, SimpleDataTypeName.DATE),
    TIMESTAMP("TIMESTAMP", Dimension.ZERO, SimpleDataTypeName.DATE),
    DATE("DATE", Dimension.ZERO, SimpleDataTypeName.DATE),
    TIME("TIME", Dimension.ZERO, SimpleDataTypeName.DATE),
    YEAR("YEAR", Dimension.ZERO, SimpleDataTypeName.DATE),

    CHAR("CHAR", Dimension.ONE, SimpleDataTypeName.STRING),
    NCHAR("NCHAR", Dimension.ONE, SimpleDataTypeName.STRING),
    CHARACTER("CHARACTER", Dimension.ONE, SimpleDataTypeName.STRING),
    VARCHAR("VARCHAR", Dimension.ONE, SimpleDataTypeName.STRING),
    NVARCHAR("NVARCHAR", Dimension.ONE, SimpleDataTypeName.STRING),
    BINARY("BINARY", Dimension.ONE, SimpleDataTypeName.STRING),
    VARBINARY("VARBINARY", Dimension.ONE, SimpleDataTypeName.STRING),

    TINYBLOB("TINYBLOB", Dimension.ZERO, SimpleDataTypeName.STRING),
    BLOB("BLOB", Dimension.ZERO, SimpleDataTypeName.STRING),
    MEDIUMBLOB("MEDIUMBLOB", Dimension.ZERO, SimpleDataTypeName.STRING),
    LONGBLOB("LONGBLOB", Dimension.ZERO, SimpleDataTypeName.STRING),
    TINYTEXT("TINYTEXT", Dimension.ZERO, SimpleDataTypeName.STRING),
    TEXT("TEXT", Dimension.ZERO, SimpleDataTypeName.STRING),
    MEDIUMTEXT("MEDIUMTEXT", Dimension.ZERO, SimpleDataTypeName.STRING),
    LONGTEXT("LONGTEXT", Dimension.ZERO, SimpleDataTypeName.STRING),

    JSON("JSON", Dimension.ZERO, SimpleDataTypeName.STRING),

    GEOMETRY("GEOMETRY", Dimension.ZERO, SimpleDataTypeName.STRING),

    POINT("POINT", Dimension.ZERO, SimpleDataTypeName.STRING),

    LINESTRING("LINESTRING", Dimension.ZERO, SimpleDataTypeName.STRING),

    POLYGON("POLYGON", Dimension.ZERO, SimpleDataTypeName.STRING),

    MULTIPOINT("MULTIPOINT", Dimension.ZERO, SimpleDataTypeName.STRING),

    MULTILINESTRING("MULTILINESTRING", Dimension.ZERO, SimpleDataTypeName.STRING),

    MULTIPOLYGON("MULTIPOLYGON", Dimension.ZERO, SimpleDataTypeName.STRING),

    GEOMETRYCOLLECTION("GEOMETRYCOLLECTION", Dimension.ZERO, SimpleDataTypeName.STRING);

    private final String value;

    private final Dimension dimension;

    private final SimpleDataTypeName simpleDataTypeName;

    OceanBaseMysqlDataTypeName(String value, Dimension dimension, SimpleDataTypeName simpleDataTypeName) {
        this.value = value;
        this.dimension = dimension;
        this.simpleDataTypeName = simpleDataTypeName;
    }

    public static IDataTypeName getByValue(String name) {
        OceanBaseMysqlDataTypeName[] oceanBaseMysqlDataTypeNames = OceanBaseMysqlDataTypeName.values();
        for (OceanBaseMysqlDataTypeName oceanBaseMysqlDataTypeName : oceanBaseMysqlDataTypeNames) {
            if (StringUtils.equalsIgnoreCase(oceanBaseMysqlDataTypeName.getValue(), name)) {
                return oceanBaseMysqlDataTypeName;
            }
        }
        return null;
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

    @Override
    public SimpleDataTypeName getSimpleDataTypeName() {
        return simpleDataTypeName;
    }
}
