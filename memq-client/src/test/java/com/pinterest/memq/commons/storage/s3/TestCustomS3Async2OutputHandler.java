/**
 * Copyright 2022 Pinterest, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.pinterest.memq.commons.storage.s3;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.Test;

import com.codahale.metrics.MetricRegistry;
import com.pinterest.memq.core.commons.Message;

public class TestCustomS3Async2OutputHandler {

  @Test
  public void testAnyUploadResultOrTimeout() throws Exception {
    CustomS3Async2StorageHandler handler = new CustomS3Async2StorageHandler();
    Properties props = new Properties();
    props.setProperty("bucket", "test");
    handler.initWriter(props, "test", new MetricRegistry());
    List<CompletableFuture<CustomS3Async2StorageHandler.UploadResult>> tasks = new ArrayList<>();

    for (int i = 1; i <= 5; i++) {
      final int j = i;
      tasks.add(CompletableFuture.supplyAsync(() -> {
        try {
          Thread.sleep(200 * j);
        } catch (Exception e) {
        }
        return new CustomS3Async2StorageHandler.UploadResult("task-" + j, 200, null, 0, j);
      }));
    }

    try {
      CustomS3Async2StorageHandler.UploadResult r = handler
          .anyUploadResultOrTimeout(tasks, Duration.ofMillis(1000)).get();
      assertEquals(r.getKey(), "task-1");
    } catch (Exception e) {
      fail("Should not fail: " + e);
    }

    tasks.clear();

    for (int i = 1; i <= 5; i++) {
      final int j = i;
      tasks.add(CompletableFuture.supplyAsync(() -> {
        try {
          Thread.sleep(200 * j + 1000);
        } catch (Exception e) {
        }
        return new CustomS3Async2StorageHandler.UploadResult("task-" + j, 200, null, 0, j);
      }));
    }

    try {
      CustomS3Async2StorageHandler.UploadResult r = handler
          .anyUploadResultOrTimeout(tasks, Duration.ofMillis(1000)).get();
      fail("Should timeout");
    } catch (ExecutionException ee) {
      System.out.println(ee);
      assertTrue(ee.getCause() instanceof TimeoutException);
    } catch (Exception e) {
      fail("Should throw timeout exception");
    }
  }

  @Test
  public void testAsyncUpload() throws Exception {
    CustomS3Async2StorageHandler handler = new CustomS3Async2StorageHandler();
    Properties props = new Properties();
    props.setProperty("bucket", "test");
    props.setProperty("retryTimeoutMillis", "500");
    handler.initWriter(props, "test", new MetricRegistry());

    Message m1 = new Message(1024, false);
    m1.put("adasdas".getBytes("utf-8"));
    Message m2 = new Message(512, false);
    m2.put("abcdefgh".getBytes("utf-8"));
    Message m3 = new Message(128, false);
    m3.put("123456789".getBytes("utf-8"));
    List<Message> messages = Arrays.asList(m1, m2, m3);

    CompletableFuture<Void> future = new CompletableFuture<Void>();
    int sizeInBytes = messages.stream().mapToInt(b -> b.getBuf().writerIndex()).sum();
    for (int i = 0; i < 30; i++) {
      final int k = i;
      handler.writeOutputAsync(sizeInBytes, 0, messages, (ex) -> {
        if (ex != null) {
          ex.printStackTrace();
        } else {
          System.out.println("Request completed:" + k);
        }
      });
    }
    handler.writeOutputAsync(sizeInBytes, 0, messages, (ex) -> {
      if (ex != null) {
        ex.printStackTrace();
      } else {
        System.out.println("Request completed");
      }
    });
    future.get();
    handler.getExecutionTimer().shutdown();
    handler.getExecutionTimer().awaitTermination(10, TimeUnit.SECONDS);
  }
}