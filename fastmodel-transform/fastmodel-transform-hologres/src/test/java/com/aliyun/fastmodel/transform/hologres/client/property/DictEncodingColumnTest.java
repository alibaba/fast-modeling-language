/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.hologres.client.property;

import java.util.List;

import com.aliyun.fastmodel.transform.hologres.client.property.DictEncodingColumn.ColumnStatus;
import com.aliyun.fastmodel.transform.hologres.client.property.DictEncodingColumn.Status;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2022/6/16
 */
public class DictEncodingColumnTest {

    @Test
    public void setValueString() {
        DictEncodingColumn dictEncodingColumns = new DictEncodingColumn();
        dictEncodingColumns.setValueString("c1:on,c2:off");
        List<ColumnStatus> value = dictEncodingColumns.getValue();
        assertEquals(value.size(), 2);
        ColumnStatus columnStatus = value.get(0);
        assertEquals(columnStatus.getStatus(), Status.ON);
    }

    @Test
    public void setValueStringWithoutStatus() {
        DictEncodingColumn dictEncodingColumns = new DictEncodingColumn();
        dictEncodingColumns.setValueString("c1,c2");
        List<ColumnStatus> value = dictEncodingColumns.getValue();
        assertEquals(value.size(), 2);
        ColumnStatus columnStatus = value.get(0);
        assertEquals(columnStatus.getStatus(), Status.AUTO);
    }
}