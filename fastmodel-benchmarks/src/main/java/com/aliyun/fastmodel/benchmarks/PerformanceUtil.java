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

package com.aliyun.fastmodel.benchmarks;

import com.google.common.base.Stopwatch;
import java.time.Duration;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

/**
 * performance util
 *
 * @author panguanjing
 * @date 2021/4/1
 */
@Slf4j
public class PerformanceUtil {
    /**
     * create executor
     *
     * @param numOfThreads
     * @param second
     * @param queueSize
     * @return
     */
    public static ExecutorService createExecutor(int numOfThreads, long second, Integer queueSize) {
        return new ThreadPoolExecutor(
            numOfThreads, numOfThreads,
            second,
            TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(queueSize),
            r -> {
                Thread t = new Thread("PerformanceTest-Thread");
                t.setDaemon(true);
                return t;
            }
        );
    }

    public static Long getCost(PerformanceExec performanceExec, TimeUnit timeUnit) {
        Stopwatch stopwatch = Stopwatch.createStarted();
        performanceExec.execute();
        Duration elapsed = stopwatch.elapsed();
        if (timeUnit == TimeUnit.MILLISECONDS) {
            return elapsed.toMillis();
        } else if (timeUnit == TimeUnit.NANOSECONDS) {
            return Integer.valueOf(elapsed.getNano()).longValue();
        }
        throw new IllegalArgumentException("unsupported timeunit with:" + timeUnit);
    }

}
