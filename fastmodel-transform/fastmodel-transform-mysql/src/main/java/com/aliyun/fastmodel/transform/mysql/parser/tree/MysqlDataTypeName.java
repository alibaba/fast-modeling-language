package com.aliyun.fastmodel.transform.mysql.parser.tree;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

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
     * real, 默认等价于double
     */
    REAL("REAL", Dimension.TWO, SimpleDataTypeName.NUMBER),

    /**
     * bit
     */
    BIT("BIT", Dimension.ONE, SimpleDataTypeName.NUMBER),

    /**
     * boolean
     */
    BOOLEAN("BOOLEAN", Dimension.ZERO, SimpleDataTypeName.BOOLEAN),

    /**
     * serial, 等价于 BIGINT UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE
     */
    SERIAL("SERIAL", Dimension.ZERO, SimpleDataTypeName.NUMBER),

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
     * nchar
     */
    NCHAR("NCHAR", Dimension.ONE, SimpleDataTypeName.STRING),

    /**
     * nvarchar
     */
    NVARCHAR("NVARCHAR", Dimension.ONE, SimpleDataTypeName.STRING),

    /**
     * long varchar
     */
    LONG_VARCHAR("LONG VARCHAR", Dimension.ZERO, SimpleDataTypeName.STRING),

    /**
     * long varbinary
     */
    LONG_VARBINARY("LONG VARBINARY", Dimension.ZERO, SimpleDataTypeName.STRING),

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

    /**
     * 参数前缀
     */
    public static final String BRACKET_PREFIX = "(";

    /**
     * 无符号修饰符
     */
    public static final String UNSIGNED = "UNSIGNED";

    /**
     * 有符号修饰符
     */
    public static final String SIGNED = "SIGNED";

    /**
     * 零填充修饰符
     */
    public static final String ZEROFILL = "ZEROFILL";

    private static final String BLANK = " ";

    private static final String BRACKET_REGEX = "\\([^)]*\\)";

    private static final String MULTI_BLANK_REGEX = "\\s+";

    /**
     * MySQL类型别名，映射到标准类型上
     */
    private static final Map<String, MysqlDataTypeName> ALIAS;

    static {
        Map<String, MysqlDataTypeName> alias = new HashMap<>();
        alias.put("BOOL", BOOLEAN);
        alias.put("CHARACTER", CHAR);
        alias.put("DEC", DECIMAL);
        alias.put("FIXED", DECIMAL);
        alias.put("DOUBLE PRECISION", DOUBLE);
        alias.put("MIDDLEINT", MEDIUMINT);
        alias.put("INT1", TINYINT);
        alias.put("INT2", SMALLINT);
        alias.put("INT3", MEDIUMINT);
        alias.put("INT4", INT);
        alias.put("INT8", BIGINT);
        alias.put("FLOAT4", FLOAT);
        alias.put("FLOAT8", DOUBLE);
        alias.put("GEOMCOLLECTION", GEOMETRYCOLLECTION);
        alias.put("LONG", LONG_VARCHAR);
        alias.put("NATIONAL CHAR", CHAR);
        alias.put("NATIONAL CHARACTER", CHAR);
        alias.put("NATIONAL VARCHAR", VARCHAR);
        ALIAS = Collections.unmodifiableMap(alias);
    }

    private final String value;

    private final Dimension dimension;

    private final SimpleDataTypeName simpleDataTypeName;

    MysqlDataTypeName(String value, Dimension dimension,
        SimpleDataTypeName simpleDataTypeName) {
        this.value = value;
        this.dimension = dimension;
        this.simpleDataTypeName = simpleDataTypeName;
    }

    /**
     * 根据类型字符串获取类型名称，兼容带参数、带 UNSIGNED / SIGNED / ZEROFILL 修饰符的写法
     *
     * @param value 类型字符串，例如 int、int(10) unsigned、bigint unsigned zerofill
     * @return 类型名称，带修饰符时返回 {@link MysqlModifierDataTypeName}
     */
    public static IDataTypeName getByValue(String value) {
        MysqlDataTypeName base = getBaseDataTypeName(value);
        String modifiers = getModifiers(value);
        if (StringUtils.isBlank(modifiers)) {
            return base;
        }
        return new MysqlModifierDataTypeName(base, modifiers);
    }

    /**
     * 获取基础类型名称，忽略参数和修饰符
     *
     * @param value 类型字符串
     * @return 基础类型
     */
    public static MysqlDataTypeName getBaseDataTypeName(String value) {
        String v = normalize(value);
        for (MysqlDataTypeName s : MysqlDataTypeName.values()) {
            if (StringUtils.equalsIgnoreCase(s.getValue(), v)) {
                return s;
            }
        }
        MysqlDataTypeName alias = ALIAS.get(v);
        if (alias != null) {
            return alias;
        }
        throw new IllegalArgumentException("not support the dataType with value:" + value);
    }

    /**
     * 获取类型的基础名称（去除参数与修饰符），例如 int(10) unsigned 返回 INT
     *
     * @param value 类型字符串
     * @return 基础类型字符串
     */
    public static String getBaseValue(String value) {
        return getBaseDataTypeName(value).getValue();
    }

    /**
     * 获取类型修饰符，按 MySQL 语法顺序返回，例如 int(10) unsigned zerofill 返回 UNSIGNED ZEROFILL
     *
     * @param value 类型字符串
     * @return 修饰符，没有修饰符时返回空字符串
     */
    public static String getModifiers(String value) {
        if (StringUtils.isBlank(value)) {
            return StringUtils.EMPTY;
        }
        String v = value.replaceAll(BRACKET_REGEX, BLANK);
        boolean unsigned = false;
        boolean signed = false;
        boolean zerofill = false;
        for (String token : v.trim().split(MULTI_BLANK_REGEX)) {
            if (StringUtils.equalsIgnoreCase(token, UNSIGNED)) {
                unsigned = true;
            } else if (StringUtils.equalsIgnoreCase(token, SIGNED)) {
                signed = true;
            } else if (StringUtils.equalsIgnoreCase(token, ZEROFILL)) {
                zerofill = true;
            }
        }
        List<String> modifiers = new ArrayList<>();
        if (unsigned) {
            modifiers.add(UNSIGNED);
        } else if (signed) {
            modifiers.add(SIGNED);
        }
        if (zerofill) {
            modifiers.add(ZEROFILL);
        }
        return String.join(BLANK, modifiers);
    }

    /**
     * 是否包含 UNSIGNED / SIGNED / ZEROFILL 修饰符
     *
     * @param value 类型字符串
     * @return true 表示包含修饰符
     */
    public static boolean hasModifier(String value) {
        return StringUtils.isNotBlank(getModifiers(value));
    }

    private static String normalize(String value) {
        if (StringUtils.isBlank(value)) {
            throw new IllegalArgumentException("dataType can't be blank");
        }
        String v = value.trim();
        if (v.indexOf(MULTI_PREFIX) > 0) {
            v = v.substring(0, v.indexOf(MULTI_PREFIX)).trim();
        }
        if (v.indexOf(BRACKET_PREFIX) > 0) {
            v = v.substring(0, v.indexOf(BRACKET_PREFIX)).trim();
        }
        v = v.replaceAll("(?i)\\b" + UNSIGNED + "\\b", BLANK);
        v = v.replaceAll("(?i)\\b" + SIGNED + "\\b", BLANK);
        v = v.replaceAll("(?i)\\b" + ZEROFILL + "\\b", BLANK);
        return v.replaceAll(MULTI_BLANK_REGEX, BLANK).trim().toUpperCase(Locale.ROOT);
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