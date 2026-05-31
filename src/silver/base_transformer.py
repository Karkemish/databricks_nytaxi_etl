from pyspark.sql import DataFrame
from pyspark.sql.functions import col
import re

class SilverBaseTransformer:
    @staticmethod
    def standardize_column_names(df: DataFrame) -> DataFrame:
        """
        Standardize the column names of the input DataFrame.

        Args:
            df (DataFrame): The input DataFrame.

        Returns:
            DataFrame: The transformed DataFrame.
        """
        # Placeholder for transformation logic
        pattern = r'(?<=.)id$'
        new_columns = []
        for old_col in df.columns:
            new_col = re.sub(pattern, '_id', old_col)
            new_columns.append(col(old_col).alias(new_col))
        return df.select(*new_columns)