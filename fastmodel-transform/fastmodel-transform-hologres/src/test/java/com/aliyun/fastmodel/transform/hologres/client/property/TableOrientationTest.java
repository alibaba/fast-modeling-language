/*
 * Copyright (c)  2022. Aliyun.com All right reserved. This software is the
 * confidential and proprietary information of Aliyun.com ("Confidential
 * Information"). You shall not disclose such Confidential Information and shall
 * use it only in accordance with the terms of the license agreement you entered
 * into with Aliyun.com.
 */

package com.aliyun.fastmodel.transform.hologres.client.property;

import java.util.Arrays;

import com.aliyun.fastmodel.transform.hologres.client.property.TableOrientation.Orientation;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2022/6/16
 */
public class TableOrientationTest {

    @Test
    public void valueString() {
        TableOrientation tableOrientation = new TableOrientation();
        tableOrientation.setValue(Arrays.asList(Orientation.ROW.getValue()));
        assertEquals(tableOrientation.getValue().size(), 1);
    }

    @Test
    public void setValueString() {
        TableOrientation tableOrientation = new TableOrientation();
        tableOrientation.setValueString("row,column");
        assertEquals(tableOrientation.getValue().size(), 2);
    }
}