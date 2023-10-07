package com.aliyun.fastmodel.transform.sqlite.parser.datatype;

import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName;
import com.aliyun.fastmodel.transform.api.datatype.simple.ISimpleDataTypeName;
import com.aliyun.fastmodel.transform.api.datatype.simple.SimpleDataTypeName;
import lombok.Getter;

/**
 * https://www.sqlite.org/draft/datatype3.html
 *
 * @author panguanjing
 * @date 2023/8/14
 */
@Getter
public enum SqliteDataTypeName implements ISimpleDataTypeName, IDataTypeName {
    /**
     * integer
     */
    INTEGER("INTEGER", SimpleDataTypeName.NUMBER),

    /**
     * text
     */
    TEXT("TEXT", SimpleDataTypeName.STRING),

    /**
     * real
     */
    REAL("REAL", SimpleDataTypeName.NUMBER),

    /**
     * numeric
     */
    NUMERIC("NUMERIC", SimpleDataTypeName.NUMBER);

    private final String value;

    private final SimpleDataTypeName simpleDataTypeName;

    SqliteDataTypeName(String value, SimpleDataTypeName simpleDataTypeName) {
        this.value = value;
        this.simpleDataTypeName = simpleDataTypeName;
    }

    @Override
    public String getName() {
        return name();
    }

    @Override
    public String getValue() {
        return this.value;
    }

    @Override
    public Dimension getDimension() {
        return Dimension.ZERO;
    }

    @Override
    public SimpleDataTypeName getSimpleDataTypeName() {
        return simpleDataTypeName;
    }
}
