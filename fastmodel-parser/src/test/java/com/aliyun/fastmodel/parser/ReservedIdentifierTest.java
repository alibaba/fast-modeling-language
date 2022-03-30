/*
 * Copyright 2021-2022 Alibaba Group Holding Ltd.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.aliyun.fastmodel.parser;

import java.lang.reflect.Method;
import java.util.Locale;
import java.util.Set;

import com.aliyun.fastmodel.parser.generate.FastModelGrammarParser.NonReservedContext;
import com.aliyun.fastmodel.parser.lexer.ReservedIdentifier;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Desc:
 *
 * @author panguanjing
 * @date 2021/2/22
 */
public class ReservedIdentifierTest {

    @Test
    public void isKeyWord() {
        boolean order = ReservedIdentifier.isKeyWord("order");
        assertTrue(order);
        boolean order1 = ReservedIdentifier.isKeyWord("ORDER");
        assertTrue(order1);
    }

    @Test
    public void isCode() {
        boolean order = ReservedIdentifier.isKeyWord("code");
        assertTrue(order);
    }

    @Test
    public void getKeyWords() {
        Set<String> keywords = ReservedIdentifier.getKeywords();
        int size = 0 ;
        for (String k : keywords) {
            size ++ ;
            if (size % 6 == 0) {
                System.out.println("" + k.toUpperCase(Locale.ROOT) + "");
                size = 0;
            }else{
                System.out.print(String.format("%s   ", k.toUpperCase(Locale.ROOT)));
            }
        }
    }

    @Test
    public void getNonReservered() {
        Method[] methods = NonReservedContext.class.getMethods();
        int size = 0;
        for (Method m : methods) {
            String name = m.getName();
            if (name.startsWith("KW")) {
                size ++;
                String real = name.substring(3);
                if (size % 6 == 0) {
                    System.out.println("" + real.toUpperCase(Locale.ROOT) + "");
                    size = 0;
                }else{
                    System.out.print(String.format("%s   ", real.toUpperCase(Locale.ROOT)));
                }
            }
        }
    }
}