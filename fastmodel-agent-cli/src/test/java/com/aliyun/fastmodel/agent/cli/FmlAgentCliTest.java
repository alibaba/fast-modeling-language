/*
 * Copyright 2021-2022 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package com.aliyun.fastmodel.agent.cli;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

import com.aliyun.fastmodel.core.tree.BaseStatement;
import com.aliyun.fastmodel.transform.api.Transformer;
import org.junit.Test;
import picocli.CommandLine;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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

    @Test
    public void validatesSparkWithoutCommentClause() throws Exception {
        Result result = execute("validate", "--dialect", "spark", "--text",
            "CREATE TABLE dim_shop (shop_id BIGINT)");

        assertEquals(result.output, 0, result.exitCode);
        assertTrue(result.output.contains("\"ok\":true"));
        assertTrue(result.output.contains("\"dialect\":\"spark\""));
    }

    @Test
    public void validatesZenColumnList() throws Exception {
        Result result = execute("validate", "--dialect", "zen", "--text",
            "user_id\nuser_name");

        assertEquals(result.output, 0, result.exitCode);
        assertTrue(result.output.contains("\"ok\":true"));
        assertTrue(result.output.contains("\"statementTypes\":[\"ListNode\"]"));
    }

    @Test
    public void reportsResolvedVersion() throws Exception {
        Result result = execute("--version");

        assertEquals("unexpected version output: [" + result.output + "]", 0, result.exitCode);
        assertTrue("unexpected version output: [" + result.output + "]",
            result.output.startsWith("fml-agent-cli "));
        assertFalse("version must not be null: " + result.output,
            result.output.contains("null"));
        assertFalse("version must not contain unexpanded placeholders: " + result.output,
            result.output.contains("${"));
    }

    /**
     * 能力声明必须与真实行为一致：凡是 capabilities 声称支持的 transform / validate，
     * 用规范样例实际执行都必须成功，防止再次出现能力误报。
     */
    @Test
    public void advertisedCapabilitiesMatchRuntimeBehavior() throws Exception {
        Map<String, String> validateSamples = validateSamples();
        String fmlModel = "create dim table dim_shop (shop_id bigint);";

        for (Map.Entry<String, Transformer<BaseStatement>> entry
            : FmlAgentCli.transformerRegistry().entrySet()) {
            String name = entry.getKey();
            String baseName = name.split("@", 2)[0];
            Transformer<BaseStatement> transformer = entry.getValue();

            if (FmlAgentCli.advertisedTransform(transformer)) {
                Result transform = execute("transform", "--dialect", name, "--text", fmlModel);
                assertEquals("transform advertised but failed for " + name + ": " + transform.output,
                    0, transform.exitCode);
                assertTrue(transform.output.contains("\"ok\":true"));
            }
            if (FmlAgentCli.supportsNativeValidation(name, transformer)) {
                String sample = validateSamples.get(baseName);
                assertTrue("missing validate sample for " + name, sample != null);
                Result validate = execute("validate", "--dialect", name, "--text", sample);
                assertEquals("validate advertised but failed for " + name + ": " + validate.output,
                    0, validate.exitCode);
                assertTrue(validate.output.contains("\"ok\":true"));
            }
        }
    }

    private static Map<String, String> validateSamples() {
        Map<String, String> samples = new LinkedHashMap<>();
        String genericCreateTable = "CREATE TABLE dim_shop (shop_id BIGINT);";
        samples.put("fml", "create dim table dim_shop (shop_id bigint);");
        samples.put("adb-mysql", genericCreateTable);
        samples.put("adb-pg", genericCreateTable);
        samples.put("clickhouse", genericCreateTable);
        samples.put("doris", genericCreateTable);
        samples.put("flink", genericCreateTable);
        samples.put("hive", genericCreateTable);
        samples.put("hologres", genericCreateTable);
        samples.put("mysql", genericCreateTable);
        samples.put("ob-mysql", genericCreateTable);
        samples.put("oracle", "CREATE TABLE dim_shop (shop_id NUMBER(19));");
        samples.put("spark", "CREATE TABLE dim_shop (shop_id BIGINT)");
        samples.put("sqlite", "CREATE TABLE dim_shop (shop_id INTEGER);");
        samples.put("star-rocks", genericCreateTable);
        samples.put("zen", "user_id\nuser_name");
        return samples;
    }

    private Result execute(String... args) throws Exception {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        PrintStream previous = System.out;
        try {
            System.setOut(new PrintStream(output, true, StandardCharsets.UTF_8.name()));
            // 必须在重定向 System.out 之后再构造 CommandLine：picocli 默认执行策略在构造时捕获
            // System.out，execute() 期间发现 System.out 变化会用捕获的流覆盖 setOut 设置的 writer
            CommandLine commandLine = new CommandLine(new FmlAgentCli());
            commandLine.setOut(new PrintWriter(output));
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
