package edu.colorado.cires.wod.spark.w2p;

import java.io.Serializable;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class CastError implements Serializable {

  private static final long serialVersionUID = 0L;

  public static StructType structType() {
    return new StructType(new StructField[]{
        new StructField("dataset", DataTypes.StringType, false, org.apache.spark.sql.types.Metadata.empty()),
        new StructField("castNumber", DataTypes.IntegerType, false, org.apache.spark.sql.types.Metadata.empty()),
        new StructField("error", DataTypes.StringType, false, org.apache.spark.sql.types.Metadata.empty())
    });
  }

  public static Builder builder(Row row) {
    return new Builder(row);
  }

  public static Builder builder() {
    return new Builder();
  }

  public static Builder builder(CastError source) {
    return new Builder(source);
  }

  private String dataset;
  private int castNumber;
  private String error;

  /**
   * DO NOT USE
   *
   * This is only public in order to allow for Spark conversion.
   *
   * @deprecated use {@link  #builder()}
   */
  @Deprecated
  public CastError() {
  }

  private CastError(@Nonnull String dataset, int castNumber, @Nullable String error) {
    this.dataset = dataset;
    this.castNumber = castNumber;
    this.error = error;

  }

  public Row asRow() {
    return new GenericRowWithSchema(new Object[]{
        dataset,
        castNumber,
        error
    }, structType());
  }


  public String getDataset() {
    return dataset;
  }

  @Deprecated
  public void setDataset(String dataset) {
    this.dataset = dataset;
  }

  public int getCastNumber() {
    return castNumber;
  }

  @Deprecated
  public void setCastNumber(int castNumber) {
    this.castNumber = castNumber;
  }

  public String getError() {
    return error;
  }

  @Deprecated
  public void setError(String error) {
    this.error = error;
  }

  public static class Builder {

    private String dataset;
    private int castNumber;
    private String error;

    private Builder() {
    }

    private Builder(CastError source) {
      dataset = source.getDataset();
      castNumber = source.getCastNumber();
      error = source.getError();
    }

    private Builder(Row row) {
      dataset = row.getAs("dataset");
      castNumber = row.getAs("castNumber");
      error = row.getAs("error");
    }

    public Builder withDataset(String dataset) {
      this.dataset = dataset;
      return this;
    }

    public Builder withCastNumber(int castNumber) {
      this.castNumber = castNumber;
      return this;
    }

    public Builder withError(String error) {
      this.error = error;
      return this;
    }

    public CastError build() {
      return new CastError(
          dataset,
          castNumber,
          error
      );
    }
  }
}
