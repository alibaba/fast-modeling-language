/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.hive.parser.tree.datatype;

import com.aliyun.fastmodel.core.tree.datatype.DataTypeEnums;
import com.aliyun.fastmodel.core.tree.datatype.IDataTypeName;
import com.aliyun.fastmodel.core.tree.datatype.NumericParameter;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * HiveGenericDataTypeTest
 *
 * @author panguanjing
 * @date 2022/8/8
 */
public class HiveGenericDataTypeTest {

    @Test
    public void getTypeName() {
        HiveGenericDataType hiveGenericDataType = new HiveGenericDataType(
            HiveDataTypeName.CHAR, new NumericParameter("1")
        );
        IDataTypeName typeName = hiveGenericDataType.getTypeName();
        assertEquals(typeName, HiveDataTypeName.CHAR);
    }
}