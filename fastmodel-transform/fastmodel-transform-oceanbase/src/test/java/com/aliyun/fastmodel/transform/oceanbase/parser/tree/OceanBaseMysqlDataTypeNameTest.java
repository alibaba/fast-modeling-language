package com.aliyun.fastmodel.transform.oceanbase.parser.tree;

import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName;
import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName.Dimension;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2024/2/19
 */
public class OceanBaseMysqlDataTypeNameTest {
    @Test
    public void testDataTypeName() {
        IDataTypeName varchar = OceanBaseMysqlDataTypeName.getByValue("varchar");
        assertEquals(Dimension.ONE, varchar.getDimension());
    }
}