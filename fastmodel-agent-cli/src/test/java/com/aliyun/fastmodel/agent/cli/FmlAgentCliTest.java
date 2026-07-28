/*
 * Copyright 2021-2022 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package com.aliyun.fastmodel.agent.cli;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.transform.api.Transformer;
import org.junit.Test;
import picocli.CommandLine;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FmlAgentCliTest {

    @Test
    public void validatesInlineFml() throws Exception {
        Result result = execute("validate", "--text",
            "create dim table dim_shop (shop_id bigint);");

        assertEquals(0, result.exitCode);
        assertTrue(result.output.contains("\"ok\":true"));
        assertTrue(result.output.contains("\"dialect\":\"fml\""));
        assertTrue(result.output.contains("\"statementCount\":1"));
    }

    @Test
    public void transformsInlineFmlToMysql() throws Exception {
        Result result = execute("transform", "--dialect", "mysql", "--text",
            "create dim table dim_shop (shop_id bigint);");

        assertEquals(0, result.exitCode);
        assertTrue(result.output.contains("\"dialect\":\"mysql\""));
        assertTrue(result.output.contains("CREATE TABLE"));
    }

    @Test
    public void inspectsStatementTree() throws Exception {
        Result result = execute("inspect", "--text",
            "create dim table dim_shop (shop_id bigint);");

        assertEquals(0, result.exitCode);
        assertTrue(result.output.contains("\"type\":\"CreateDimTable\""));
        assertTrue(result.output.contains("\"children\""));
    }

    @Test
    public void escapesJsonControlCharacters() {
        assertEquals("\"line\\n\\\"quoted\\\"\"", FmlAgentCli.quote("line\n\"quoted\""));
    }

    @Test
    public void discoversEveryPackagedTransformer() {
        Map<String, Transformer<BaseStatement>> dialects = FmlAgentCli.transformerRegistry();

        assertTrue(dialects.keySet().toString(), dialects.size() >= 19);
        assertTrue(dialects.containsKey("adb-mysql"));
        assertTrue(dialects.containsKey("adb-pg"));
        assertTrue(dialects.containsKey("clickhouse"));
        assertTrue(dialects.containsKey("doris"));
        assertTrue(dialects.containsKey("flink"));
        assertTrue(dialects.containsKey("fml"));
        assertTrue(dialects.containsKey("graph"));
        assertTrue(dialects.containsKey("hive"));
        assertTrue(dialects.containsKey("hologres"));
        assertTrue(dialects.containsKey("hologres@2.0"));
        assertTrue(dialects.containsKey("hologres@3.0"));
        assertTrue(dialects.containsKey("mysql"));
        assertTrue(dialects.containsKey("ob-mysql"));
        assertTrue(dialects.containsKey("oracle"));
        assertTrue(dialects.containsKey("plantuml"));
        assertTrue(dialects.containsKey("postgresql"));
        assertTrue(dialects.containsKey("spark"));
        assertTrue(dialects.containsKey("sqlite"));
        assertTrue(dialects.containsKey("star-rocks"));
        assertTrue(dialects.containsKey("zen"));
    }

    @Test
    public void validatesNativeMysqlDialect() throws Exception {
        Result result = execute("validate", "--dialect", "mysql", "--text",
            "CREATE TABLE dim_shop (shop_id BIGINT);");

        assertEquals(0, result.exitCode);
        assertTrue(result.output.contains("\"dialect\":\"mysql\""));
        assertTrue(result.output.contains("\"ok\":true"));
    }

    @Test
    public void transformsCompositeModelToPlantUml() throws Exception {
        Result result = execute("transform", "--dialect", "plantuml", "--text",
            "create dim table dim_shop (shop_id bigint);");

        assertEquals(0, result.exitCode);
        assertTrue(result.output.contains("@startuml"));
        assertTrue(result.output.contains("@enduml"));
    }

    @Test
    public void doesNotAdvertiseIncompletePostgresqlValidation() throws Exception {
        Result result = execute("capabilities");

        assertEquals(0, result.exitCode);
        assertTrue(result.output.contains(
            "{\"name\":\"postgresql\",\"transform\":true,\"validate\":false}"));
    }

    private Result execute(String... args) throws Exception {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        CommandLine commandLine = new CommandLine(new FmlAgentCli());
        commandLine.setOut(new PrintWriter(output));
        PrintStream previous = System.out;
        try {
            System.setOut(new PrintStream(output, true, StandardCharsets.UTF_8.name()));
            return new Result(commandLine.execute(args), output.toString(StandardCharsets.UTF_8.name()));
        } finally {
            System.setOut(previous);
        }
    }

    private static final class PrintWriter extends java.io.PrintWriter {
        private PrintWriter(ByteArrayOutputStream output) {
            super(output, true);
        }
    }

    private static final class Result {
        private final int exitCode;
        private final String output;

        private Result(int exitCode, String output) {
            this.exitCode = exitCode;
            this.output = output;
        }
    }
}
