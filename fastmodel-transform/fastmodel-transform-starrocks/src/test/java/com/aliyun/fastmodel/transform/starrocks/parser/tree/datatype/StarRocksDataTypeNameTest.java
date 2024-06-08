package com.aliyun.fastmodel.transform.starrocks.parser.tree.datatype;

import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2023/9/21
 */
public class StarRocksDataTypeNameTest {

    @Test
    public void testGetByValue() {
        IDataTypeName byValue = StarRocksDataTypeName.getByValue("array<int>");
        assertEquals(StarRocksDataTypeName.ARRAY, byValue);
        byValue = StarRocksDataTypeName.getByValue("json");
        assertEquals(StarRocksDataTypeName.JSON, byValue);

        IDataTypeName byValue1 = StarRocksDataTypeName.getByValue("map<int, string>");
        assertEquals(StarRocksDataTypeName.Map, byValue1);
    }
}