/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.utils;

import org.apache.iotdb.commons.utils.TestOnly;

import com.tngtech.archunit.ArchConfiguration;
import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.domain.JavaMethod;
import com.tngtech.archunit.core.domain.properties.CanBeAnnotated;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.core.importer.ImportOption.DoNotIncludeTests;
import com.tngtech.archunit.lang.ArchRule;
import org.apache.tsfile.annotations.MustOverride;
import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.Request;
import org.junit.runner.Result;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.methods;

public class AnnotationTest {

  @Test
  public void checkTestOnly() throws Exception {
    if (runInDedicatedHeap("checkTestOnly")) return;
    boolean resolveMissing = ArchConfiguration.get().resolveMissingDependenciesFromClassPath();
    ArchConfiguration.get().setResolveMissingDependenciesFromClassPath(false);
    try {
      JavaClasses productionClasses =
          new ClassFileImporter()
              .withImportOption(new DoNotIncludeTests())
              .importPackages("org.apache.iotdb");
      JavaClasses testClasses =
          new ClassFileImporter()
              .withImportOption(new ImportOption.OnlyIncludeTests())
              .importPackages("org.apache.iotdb");

      List<Class> testReflectedClasses = new ArrayList<>();
      for (JavaClass testClass : testClasses) {
        testReflectedClasses.add(testClass.reflect());
      }

      ArchRule rule =
          methods()
              .that()
              .areAnnotatedWith(TestOnly.class)
              .should()
              .onlyBeCalled()
              .byClassesThat()
              .belongToAnyOf(testReflectedClasses.toArray(new Class[0]))
              .orShould()
              .onlyBeCalled()
              .byMethodsThat(
                  CanBeAnnotated.Predicates.annotatedWith(TestOnly.class)); // see next section

      rule.check(productionClasses);
    } catch (OutOfMemoryError error) {
      throw new AssertionError("Architecture check could not complete", error);
    } finally {
      ArchConfiguration.get().setResolveMissingDependenciesFromClassPath(resolveMissing);
    }
  }

  @Test
  public void checkMustOverride() throws Exception {
    if (runInDedicatedHeap("checkMustOverride")) return;
    boolean resolveMissing = ArchConfiguration.get().resolveMissingDependenciesFromClassPath();
    ArchConfiguration.get().setResolveMissingDependenciesFromClassPath(false);
    try {
      JavaClasses productionClasses =
          new ClassFileImporter()
              .withImportOption(new DoNotIncludeTests())
              .importPackages("org.apache.iotdb", "org.apache.tsfile");

      Map<JavaClass, List<JavaMethod>> classAndMethodsMustOverride = new HashMap<>();
      for (JavaClass productionClass : productionClasses) {
        for (JavaMethod method : productionClass.getMethods()) {
          if (method.isAnnotatedWith(MustOverride.class)) {
            classAndMethodsMustOverride
                .computeIfAbsent(productionClass, k -> new ArrayList<>())
                .add(method);
          }
        }
      }

      List<String> failures = new ArrayList<>();
      for (JavaClass productionClass : productionClasses) {
        for (Entry<JavaClass, List<JavaMethod>> entry : classAndMethodsMustOverride.entrySet()) {
          JavaClass classWithTargetMethod = entry.getKey();
          if (productionClass.isAssignableTo(classWithTargetMethod.getName())) {
            List<JavaMethod> methodsThatMustOverride = entry.getValue();
            for (JavaMethod toBeOverride : methodsThatMustOverride) {
              boolean methodImplemented = false;
              for (JavaMethod method : productionClass.getMethods()) {
                if (method.getName().equals(toBeOverride.getName())
                    && method.getParameters().equals(toBeOverride.getParameters())) {
                  methodImplemented = true;
                  break;
                }
              }
              if (!methodImplemented) {
                failures.add(
                    String.format(
                        "Class %s does not implement method %s annotated with @MustOverride from class %s",
                        productionClass.getName(),
                        toBeOverride.getName(),
                        classWithTargetMethod.getName()));
              }
            }
          }
        }
      }

      if (!failures.isEmpty()) {
        throw new AssertionError(String.join("\n", failures));
      }
    } catch (OutOfMemoryError error) {
      throw new AssertionError("Architecture check could not complete", error);
    } finally {
      ArchConfiguration.get().setResolveMissingDependenciesFromClassPath(resolveMissing);
    }
  }

  private static boolean runInDedicatedHeap(String method) throws Exception {
    if (Runtime.getRuntime().maxMemory() >= 3L * 1024 * 1024 * 1024) return false;
    List<String> command = new ArrayList<>();
    command.add(System.getProperty("java.home") + File.separator + "bin" + File.separator + "java");
    for (String argument : ManagementFactory.getRuntimeMXBean().getInputArguments()) {
      if (argument.startsWith("--add-opens=") || argument.startsWith("--add-exports=")) {
        command.add(argument);
      }
    }
    command.add("-Xmx4g");
    command.add("-Djava.io.tmpdir=" + System.getProperty("java.io.tmpdir"));
    command.add("-cp");
    command.add(System.getProperty("java.class.path"));
    command.add(AnnotationTest.class.getName());
    command.add(method);
    Path output = Files.createTempFile("architecture-check-", ".log");
    ProcessBuilder builder =
        new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(output.toFile());
    builder.environment().remove("_JAVA_OPTIONS");
    builder.environment().remove("JAVA_TOOL_OPTIONS");
    builder.environment().remove("JDK_JAVA_OPTIONS");
    Process child = builder.start();
    try {
      if (!child.waitFor(120, TimeUnit.SECONDS)) {
        throw new AssertionError("Architecture check exceeded its 120 second deadline");
      }
      System.out.print(Files.readString(output));
      if (child.exitValue() != 0) {
        throw new AssertionError("Architecture check child failed: " + method);
      }
      return true;
    } finally {
      if (child.isAlive()) child.destroyForcibly();
      Files.deleteIfExists(output);
    }
  }

  public static void main(String[] args) {
    System.out.println(
        "Architecture check child: method="
            + args[0]
            + " maxHeapBytes="
            + Runtime.getRuntime().maxMemory()
            + " classSource="
            + AnnotationTest.class.getProtectionDomain().getCodeSource().getLocation());
    Result result = new JUnitCore().run(Request.method(AnnotationTest.class, args[0]));
    result.getFailures().forEach(failure -> System.err.println(failure.getTrace()));
    if (!result.wasSuccessful()) throw new AssertionError("Architecture rule failed in child JVM");
  }
}
