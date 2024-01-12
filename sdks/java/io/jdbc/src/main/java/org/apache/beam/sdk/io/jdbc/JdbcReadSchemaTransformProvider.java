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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.jdbc;

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.vendor.grpc.v1p54p0.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

/**
 * An implementation of {@link org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider} for
 * reading from JDBC connections using {@link org.apache.beam.sdk.io.jdbc.JdbcIO}.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
@AutoService(SchemaTransformProvider.class)
public class JdbcReadSchemaTransformProvider
    extends TypedSchemaTransformProvider<
        JdbcReadSchemaTransformProvider.JdbcReadSchemaTransformConfiguration> {

  private static final List<String> JDBCTypes =
      ImmutableList.of("mysql", "postgres", "oracle", "mssql");

  @Override
  protected @UnknownKeyFor @NonNull @Initialized Class<JdbcReadSchemaTransformConfiguration>
      configurationClass() {
    return JdbcReadSchemaTransformConfiguration.class;
  }

  @Override
  protected @UnknownKeyFor @NonNull @Initialized SchemaTransform from(
      JdbcReadSchemaTransformConfiguration configuration) {
    configuration.validate();
    return new JdbcReadSchemaTransform(configuration);
  }

  static class JdbcReadSchemaTransform extends SchemaTransform implements Serializable {

    JdbcReadSchemaTransformConfiguration config;

    public JdbcReadSchemaTransform(JdbcReadSchemaTransformConfiguration config) {
      this.config = config;
    }

    protected JdbcIO.DataSourceConfiguration dataSourceConfiguration() {
      String driverClassName = config.getDriverClassName();

      if (Strings.isNullOrEmpty(driverClassName)) {
        switch (Objects.requireNonNull(config.getJdbcType()).toLowerCase()) {
          case "mysql":
            driverClassName = "com.mysql.cj.jdbc.Driver";
            break;
          case "postgres":
            driverClassName = "org.postgresql.Driver";
            break;
          case "oracle":
            driverClassName = "oracle.jdbc.driver.OracleDriver";
            break;
          case "mssql":
            driverClassName = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
            break;
          default:
            throw new IllegalArgumentException("JDBC type must be one of " + JDBCTypes);
        }
      }

      JdbcIO.DataSourceConfiguration dsConfig =
          JdbcIO.DataSourceConfiguration.create(driverClassName, config.getJdbcUrl())
              .withUsername("".equals(config.getUsername()) ? null : config.getUsername())
              .withPassword("".equals(config.getPassword()) ? null : config.getPassword());
      String connectionProperties = config.getConnectionProperties();
      if (connectionProperties != null) {
        dsConfig = dsConfig.withConnectionProperties(connectionProperties);
      }

      List<@org.checkerframework.checker.nullness.qual.Nullable String> initialSql =
          config.getConnectionInitSql();
      if (initialSql != null && initialSql.size() > 0) {
        dsConfig = dsConfig.withConnectionInitSqls(initialSql);
      }

      String driverJars = config.getDriverJars();
      if (driverJars != null) {
        dsConfig = dsConfig.withDriverJars(config.getDriverJars());
      }

      return dsConfig;
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
      String query = config.getReadQuery();
      if (query == null) {
        query = String.format("SELECT * FROM %s", config.getLocation());
      }
      JdbcIO.ReadRows readRows =
          JdbcIO.readRows().withDataSourceConfiguration(dataSourceConfiguration()).withQuery(query);
      Short fetchSize = config.getFetchSize();
      if (fetchSize != null && fetchSize > 0) {
        readRows = readRows.withFetchSize(fetchSize);
      }
      Boolean outputParallelization = config.getOutputParallelization();
      if (outputParallelization != null) {
        readRows = readRows.withOutputParallelization(outputParallelization);
      }
      return PCollectionRowTuple.of("output", input.getPipeline().apply(readRows));
    }
  }

  @Override
  public @UnknownKeyFor @NonNull @Initialized String identifier() {
    return "beam:schematransform:org.apache.beam:jdbc_read:v1";
  }

  @Override
  public @UnknownKeyFor @NonNull @Initialized List<@UnknownKeyFor @NonNull @Initialized String>
      inputCollectionNames() {
    return Collections.emptyList();
  }

  @Override
  public @UnknownKeyFor @NonNull @Initialized List<@UnknownKeyFor @NonNull @Initialized String>
      outputCollectionNames() {
    return Collections.singletonList("output");
  }

  @AutoValue
  @DefaultSchema(AutoValueSchema.class)
  public abstract static class JdbcReadSchemaTransformConfiguration implements Serializable {
    @Nullable
    public abstract String getDriverClassName();

    @Nullable
    public abstract String getJdbcType();

    public abstract String getJdbcUrl();

    @Nullable
    public abstract String getUsername();

    @Nullable
    public abstract String getPassword();

    @Nullable
    public abstract String getConnectionProperties();

    @Nullable
    public abstract List<@org.checkerframework.checker.nullness.qual.Nullable String>
        getConnectionInitSql();

    @Nullable
    public abstract String getReadQuery();

    @Nullable
    public abstract String getLocation();

    @Nullable
    public abstract Short getFetchSize();

    @Nullable
    public abstract Boolean getOutputParallelization();

    @Nullable
    public abstract String getDriverJars();

    public void validate() throws IllegalArgumentException {
      if (Strings.isNullOrEmpty(getDriverClassName()) && Strings.isNullOrEmpty(getJdbcType())) {
        throw new IllegalArgumentException(
            "Either JDBC Driver class name or JDBC type must be set.");
      }
      if (Strings.isNullOrEmpty(getJdbcUrl())) {
        throw new IllegalArgumentException("JDBC URL cannot be blank");
      }
      if (!Strings.isNullOrEmpty(getJdbcType())
          && !JDBCTypes.contains(getJdbcType().toLowerCase())) {
        throw new IllegalArgumentException("JDBC type must be one of " + JDBCTypes);
      }

      boolean readQueryPresent = (getReadQuery() != null && !"".equals(getReadQuery()));
      boolean locationPresent = (getLocation() != null && !"".equals(getLocation()));

      if (readQueryPresent && locationPresent) {
        throw new IllegalArgumentException(
            "ReadQuery and Location are mutually exclusive configurations");
      }
      if (!readQueryPresent && !locationPresent) {
        throw new IllegalArgumentException("Either ReadQuery or Location must be set.");
      }
    }

    public static Builder builder() {
      return new AutoValue_JdbcReadSchemaTransformProvider_JdbcReadSchemaTransformConfiguration
          .Builder();
    }

    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setDriverClassName(String value);

      public abstract Builder setJdbcType(String value);

      public abstract Builder setJdbcUrl(String value);

      public abstract Builder setUsername(String value);

      public abstract Builder setPassword(String value);

      public abstract Builder setLocation(String value);

      public abstract Builder setReadQuery(String value);

      public abstract Builder setConnectionProperties(String value);

      public abstract Builder setConnectionInitSql(List<String> value);

      public abstract Builder setFetchSize(Short value);

      public abstract Builder setOutputParallelization(Boolean value);

      public abstract Builder setDriverJars(String value);

      public abstract JdbcReadSchemaTransformConfiguration build();
    }
  }
}
