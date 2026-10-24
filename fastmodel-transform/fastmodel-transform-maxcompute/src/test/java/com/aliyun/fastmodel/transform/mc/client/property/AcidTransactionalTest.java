package com.aliyun.fastmodel.transform.mc.client.property;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2023/2/19
 */
public class AcidTransactionalTest {

    @Test
    public void testSetValue() {
        AcidTransactional transactional = new AcidTransactional();
        transactional.setValueString("true");
        assertTrue(transactional.getValue());
        transactional.setValueString("false");
        assertFalse(transactional.getValue());
    }
}