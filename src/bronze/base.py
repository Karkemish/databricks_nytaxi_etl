from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp, input_file_name

class BronzeBasePipeline:
    @staticmethod
    def add_audit_columns(df: DataFrame) -> DataFrame:
        """
        Add metadata columns to the DataFrame.

        Args:
            df (DataFrame): The input DataFrame.

        Returns:
            DataFrame: The DataFrame with added metadata columns.
        """
        return (df
                .withColumn("ingested_at", current_timestamp())
                .withColumn("source_file", input_file_name()))