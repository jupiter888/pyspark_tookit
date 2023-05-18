# EDA PYSPARK

# row count
df.count()

# df column names
column_names = df.columns
print(column_names)

# df column datatypes - returns a list of tuples col_name:data_type
column_types = df.dtypes
print(column_types)

# numeric column's summary stats
df.describe().show()

# specific column's summary stats
df.select('column_name').summary().show()

# counting number of distinct values in column 
# count of unique values present in the specified column, eliminating any duplicates
df.select('column_name').distinct().count()

# frequency distribution 
# how many times each unique value appears in the df column
df.groupBy('column_name').count().show()

# computing the correlation matrix between numerical columns
from pyspark.ml.stat import Correlation
correlation_matrix = Correlation.corr(df, 'features').head()
print(correlation_matrix[0])

# count missing values in each column
df.select([count(when(isnull(c), c)).alias(c) for c in df.columns]).show()

# count missing values in specific column 
from pyspark.sql.functions import col, sum
# Count missing values in a column
column_name = "your_column_name"
missing_count = df.where(col(column_name).isNull()).count()
print(f"Missing values in column '{column_name}': {missing_count}")


# drop rows with any null vals
df.dropna()

# filling null vals with a specific value
df.fillna({'column_name': 'value'})


# replace null values with the column mean (imputation)
mean_values = df.agg({'column_name': 'mean'}).first()
df.fillna(mean_values, subset=['column_name'])
# alternatively 
# IMPUTATION with column MEAN
from pyspark.sql.functions import mean
# calculate the column mean
mean_value = df.select(mean(column_name)).first()[0]
# impute missing values with the mean
df_filled = df.fillna({column_name: mean_value})


# IMPUTATION with column MEDIAN 
# Pyspark does not have a median function, this is using numpy 
import numpy as np
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import DoubleType
# define a UDF to calculate median
@pandas_udf(DoubleType(), PandasUDFType.SCALAR)
def median_udf(v):
    return np.nanmedian(v)
# calculate the column median
median_value = df.select(median_udf(column_name)).first()[0]
# impute missing values with the median
df_filled = df.fillna({column_name: median_value})

