/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.formats;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

import org.apache.iceberg.FileFormat;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.util.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestFormatModelRegistry {

  @BeforeEach
  void clearRegistry() {
    FormatModelRegistry.models().clear();
  }

  @Test
  void testSuccessfulRegister() {
    FormatModel<?, ?> model = new DummyParquetFormatModel(Object.class, Object.class);
    FormatModelRegistry.register(model);
    assertThat(FormatModelRegistry.models())
        .containsEntry(Pair.of(FileFormat.PARQUET, Object.class), model);
  }

  /** Tests that registering the same class with the same configuration updates the registration. */
  @Test
  void testRegistrationForDifferentType() {
    FormatModel<?, ?> model1 = new DummyParquetFormatModel(Object.class, Object.class);
    FormatModel<?, ?> model2 = new DummyParquetFormatModel(Long.class, Object.class);
    FormatModelRegistry.register(model1);
    assertThat(FormatModelRegistry.models().get(Pair.of(FileFormat.PARQUET, model1.type())))
        .isSameAs(model1);

    // Registering a new model with the different format will succeed
    FormatModelRegistry.register(model2);
    assertThat(FormatModelRegistry.models().get(Pair.of(FileFormat.PARQUET, model1.type())))
        .isSameAs(model1);
    assertThat(FormatModelRegistry.models().get(Pair.of(FileFormat.PARQUET, model2.type())))
        .isSameAs(model2);
  }

  /**
   * Tests that registering different classes, or different schema type for the same file format and
   * type is failing.
   */
  @Test
  void testFailingReRegistrations() {
    FormatModel<?, ?> model = new DummyParquetFormatModel(Object.class, Object.class);
    FormatModelRegistry.register(model);
    assertThat(FormatModelRegistry.models())
        .containsEntry(Pair.of(FileFormat.PARQUET, Object.class), model);

    // Registering a new model with different schema type should fail
    assertThatThrownBy(
            () ->
                FormatModelRegistry.register(
                    new DummyParquetFormatModel(Object.class, String.class)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot register class");

    // Registering a new model with null schema type should fail
    assertThatThrownBy(
            () -> FormatModelRegistry.register(new DummyParquetFormatModel(Object.class, null)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot register class");
  }

  @Test
  void testRegisterSupportedFormatsHandlesLinkageError() {
    // registerSupportedFormats must not propagate LinkageError thrown when a class that is present
    // on the compile-time classpath cannot be linked at runtime due to a missing dependency (e.g.
    // iceberg-orc absent while iceberg-data is present).
    FormatModelRegistry.registerSupportedFormats(
        ImmutableList.of(ThrowsLinkageErrorOnRegister.class.getName()));
    // no exception means the error was swallowed as intended
  }

  @Test
  void testRegisterSupportedFormatsHandlesNoSuchMethod() {
    // Classes that exist on the classpath but have no register() method must be skipped silently.
    FormatModelRegistry.registerSupportedFormats(ImmutableList.of(String.class.getName()));
  }

  /** Helper class whose {@code register()} method throws {@link NoClassDefFoundError}. */
  public static class ThrowsLinkageErrorOnRegister {
    public static void register() {
      throw new NoClassDefFoundError("org.apache.iceberg.orc.ORCFormatModel");
    }
  }

  private static class DummyParquetFormatModel implements FormatModel<Object, Object> {
    private final Class<?> type;
    private final Class<?> schemaType;

    private DummyParquetFormatModel(Class<?> type, Class<?> schemaType) {
      this.type = type;
      this.schemaType = schemaType;
    }

    @Override
    public FileFormat format() {
      return FileFormat.PARQUET;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Class<Object> type() {
      return (Class<Object>) type;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Class<Object> schemaType() {
      return (Class<Object>) schemaType;
    }

    @Override
    public ModelWriteBuilder<Object, Object> writeBuilder(EncryptedOutputFile outputFile) {
      return null;
    }

    @Override
    public ReadBuilder<Object, Object> readBuilder(InputFile inputFile) {
      return null;
    }
  }
}
