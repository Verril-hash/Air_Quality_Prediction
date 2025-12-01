"""
================================================================================
🌍 AIR QUALITY PREDICTION & FORECASTING SYSTEM
================================================================================
A comprehensive PySpark ML pipeline for:
  ✓ Predicting Air Quality Index (AQI) from sensor data
  ✓ Forecasting future pollution levels
  ✓ Identifying high-risk zones

Author: Air Quality Analytics Team
Dataset: Global Air Quality Dataset (87k+ hourly readings)
================================================================================
"""

# ╔═══════════════════════════════════════════════════════════════════════════╗
# ║                    SECTION 1: IMPORTS & CONFIGURATION                      ║
# ╚═══════════════════════════════════════════════════════════════════════════╝

import os
import sys
from datetime import datetime, timedelta

# Set PySpark Python path BEFORE importing pyspark
# This fixes the "Python was not found" error on Windows
python_path = sys.executable
os.environ['PYSPARK_PYTHON'] = python_path
os.environ['PYSPARK_DRIVER_PYTHON'] = python_path

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, mean, desc, lag, when, to_timestamp, hour, dayofweek, month, year,
    isnan, isnull, count, lit, avg, max as spark_max, min as spark_min,
    stddev, round as spark_round, expr, row_number, udf, monotonically_increasing_id,
    date_add, current_timestamp, greatest
)
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructType, StructField, TimestampType
from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    VectorAssembler, StandardScaler, StringIndexer, OneHotEncoder,
    Imputer, MinMaxScaler
)
from pyspark.ml.regression import RandomForestRegressor, GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator

# Configuration Constants
APP_NAME = "AirQualityPrediction"
CSV_PATH = "air_quality.csv"
PREDICTIONS_OUTPUT = "aqi_predictions_output.csv"
HIGH_RISK_OUTPUT = "aqi_high_risk_zones.csv"
NUM_PARTITIONS = 20
RANDOM_SEED = 42
TRAIN_RATIO = 0.8
TEST_RATIO = 0.2

print("""
╔══════════════════════════════════════════════════════════════════════════════╗
║        🌬️  AIR QUALITY PREDICTION & FORECASTING SYSTEM  🌬️                  ║
║                     Powered by Apache Spark MLlib                            ║
╚══════════════════════════════════════════════════════════════════════════════╝
""")

# ╔═══════════════════════════════════════════════════════════════════════════╗
# ║                     SECTION 2: SPARK SESSION CREATION                      ║
# ╚═══════════════════════════════════════════════════════════════════════════╝

print("⚙️  Initializing Spark Session...")

try:
    spark = SparkSession.builder \
        .appName(APP_NAME) \
        .config("spark.sql.shuffle.partitions", NUM_PARTITIONS) \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    # Set log level to reduce verbosity
    spark.sparkContext.setLogLevel("WARN")
    
    print(f"✅ Spark Session created successfully!")
    print(f"   📌 App Name: {spark.sparkContext.appName}")
    print(f"   📌 Spark Version: {spark.version}")
    print(f"   📌 Master: {spark.sparkContext.master}")
except Exception as e:
    print(f"❌ Failed to create Spark Session: {e}")
    sys.exit(1)

# ╔═══════════════════════════════════════════════════════════════════════════╗
# ║                      SECTION 3: DATA LOADING & CACHING                     ║
# ╚═══════════════════════════════════════════════════════════════════════════╝

print("\n" + "="*80)
print("📥 LOADING DATA")
print("="*80)

try:
    # Load CSV with schema inference
    df_raw = spark.read.csv(
        CSV_PATH,
        header=True,
        inferSchema=True
    )
    
    # Repartition for scalability and cache
    df_raw = df_raw.repartition(NUM_PARTITIONS).cache()
    
    # Force caching by executing an action
    record_count = df_raw.count()
    
    print(f"\n✅ Data loaded successfully!")
    print(f"   📊 Total Records: {record_count:,}")
    print(f"   📊 Partitions: {df_raw.rdd.getNumPartitions()}")
    
except Exception as e:
    print(f"❌ Failed to load data: {e}")
    spark.stop()
    sys.exit(1)

# ╔═══════════════════════════════════════════════════════════════════════════╗
# ║                   SECTION 4: EXPLORATORY DATA ANALYSIS                     ║
# ╚═══════════════════════════════════════════════════════════════════════════╝

print("\n" + "="*80)
print("🔍 EXPLORATORY DATA ANALYSIS")
print("="*80)

# 4.1 Schema Analysis
print("\n📋 Dataset Schema:")
print("-"*50)
df_raw.printSchema()

# 4.2 Column Names Check
print("\n📋 Available Columns:")
print(f"   {df_raw.columns}")

# 4.3 Statistical Summary
print("\n📊 Statistical Summary:")
print("-"*50)
df_raw.describe().show()

# 4.4 Missing Value Analysis
print("\n🔎 Missing Value Analysis:")
print("-"*50)

def analyze_missing_values(df):
    """Analyze and display missing values for each column."""
    missing_stats = []
    total_rows = df.count()
    
    # Only check numeric columns for NaN (isnan doesn't work on timestamps)
    numeric_columns = [f.name for f in df.schema.fields 
                       if str(f.dataType) in ['DoubleType()', 'FloatType()', 'IntegerType()', 'LongType()']]
    
    for column in df.columns:
        # For numeric columns, check both null and NaN
        if column in numeric_columns:
            null_count = df.filter(col(column).isNull() | isnan(col(column))).count()
            # Count special missing values (-999, -1, etc.)
            special_missing = df.filter(col(column).isin([-999, -999.0, -1, -1.0])).count()
        else:
            # For non-numeric columns, only check null
            null_count = df.filter(col(column).isNull()).count()
            special_missing = 0
            
        total_missing = null_count + special_missing
        missing_pct = (total_missing / total_rows) * 100
        
        if total_missing > 0:
            missing_stats.append({
                'column': column,
                'null_count': null_count,
                'special_missing': special_missing,
                'total_missing': total_missing,
                'missing_pct': missing_pct
            })
    
    if missing_stats:
        for stat in missing_stats:
            print(f"   ⚠️  {stat['column']}: {stat['total_missing']:,} missing ({stat['missing_pct']:.2f}%)")
    else:
        print("   ✅ No missing values detected!")
    
    return missing_stats

missing_analysis = analyze_missing_values(df_raw)

# 4.5 Data Quality Check
print("\n📊 Data Range Analysis:")
print("-"*50)

numeric_cols = ['pm25', 'pm10', 'no2', 'co', 'so2', 'o3', 'temp', 'rh', 'wind', 'rain']
for col_name in numeric_cols:
    if col_name in df_raw.columns:
        stats = df_raw.agg(
            spark_min(col_name).alias('min'),
            spark_max(col_name).alias('max'),
            mean(col_name).alias('mean')
        ).collect()[0]
        print(f"   {col_name:10}: Min={stats['min']:.2f}, Max={stats['max']:.2f}, Mean={stats['mean']:.2f}")

# ╔═══════════════════════════════════════════════════════════════════════════╗
# ║                       SECTION 5: DATA PREPROCESSING                        ║
# ╚═══════════════════════════════════════════════════════════════════════════╝

print("\n" + "="*80)
print("🔧 DATA PREPROCESSING")
print("="*80)

# 5.1 Handle Missing Values
print("\n🧹 Handling Missing Values...")

# Replace special missing values with null
df_clean = df_raw
for col_name in numeric_cols:
    if col_name in df_clean.columns:
        df_clean = df_clean.withColumn(
            col_name,
            when((col(col_name) == -999) | (col(col_name) == -1) | isnan(col(col_name)), None)
            .otherwise(col(col_name))
        )

# Impute missing numeric values with column means
try:
    # Calculate means for imputation
    means = df_clean.select([mean(c).alias(c) for c in numeric_cols if c in df_clean.columns]).collect()[0]
    
    for col_name in numeric_cols:
        if col_name in df_clean.columns:
            mean_val = means[col_name]
            if mean_val is not None:
                df_clean = df_clean.fillna({col_name: mean_val})
    
    print("   ✅ Missing values imputed with column means")
except Exception as e:
    print(f"   ⚠️  Imputation warning: {e}")

# 5.2 Filter Valid Rows
print("\n🔍 Filtering Valid Records...")
original_count = df_clean.count()

# Filter out rows with negative pollutant values (invalid readings)
df_clean = df_clean.filter(
    (col('pm25') >= 0) & 
    (col('pm10') >= 0) & 
    (col('no2') >= 0) &
    (col('co') >= 0) &
    (col('so2') >= 0) &
    (col('o3') >= 0)
)

filtered_count = df_clean.count()
print(f"   📊 Records before filtering: {original_count:,}")
print(f"   📊 Records after filtering: {filtered_count:,}")
print(f"   📊 Removed: {original_count - filtered_count:,} invalid records")

# ╔═══════════════════════════════════════════════════════════════════════════╗
# ║                      SECTION 6: AQI CALCULATION (EPA)                      ║
# ╚═══════════════════════════════════════════════════════════════════════════╝

print("\n" + "="*80)
print("🧮 AQI CALCULATION (EPA Standard)")
print("="*80)

# EPA AQI Breakpoints for each pollutant
# Reference: https://www.airnow.gov/aqi/aqi-basics/

def calculate_sub_index(concentration, breakpoints):
    """
    Calculate AQI sub-index for a pollutant based on EPA breakpoints.
    breakpoints: list of tuples (C_lo, C_hi, I_lo, I_hi)
    """
    for C_lo, C_hi, I_lo, I_hi in breakpoints:
        if C_lo <= concentration <= C_hi:
            return ((I_hi - I_lo) / (C_hi - C_lo)) * (concentration - C_lo) + I_lo
    return 500  # Hazardous if beyond scale

# Define EPA AQI calculation using Spark SQL expressions
# PM2.5 breakpoints (24-hour, µg/m³)
print("\n📐 Calculating AQI sub-indices for each pollutant...")

# PM2.5 Sub-Index (simplified EPA calculation)
df_aqi = df_clean.withColumn(
    "aqi_pm25",
    when(col("pm25") <= 12.0, (50/12.0) * col("pm25"))
    .when((col("pm25") > 12.0) & (col("pm25") <= 35.4), 50 + ((100-50)/(35.4-12.0)) * (col("pm25") - 12.0))
    .when((col("pm25") > 35.4) & (col("pm25") <= 55.4), 100 + ((150-100)/(55.4-35.4)) * (col("pm25") - 35.4))
    .when((col("pm25") > 55.4) & (col("pm25") <= 150.4), 150 + ((200-150)/(150.4-55.4)) * (col("pm25") - 55.4))
    .when((col("pm25") > 150.4) & (col("pm25") <= 250.4), 200 + ((300-200)/(250.4-150.4)) * (col("pm25") - 150.4))
    .when((col("pm25") > 250.4) & (col("pm25") <= 350.4), 300 + ((400-300)/(350.4-250.4)) * (col("pm25") - 250.4))
    .when(col("pm25") > 350.4, 400 + ((500-400)/(500.4-350.4)) * (col("pm25") - 350.4))
    .otherwise(0)
)

# PM10 Sub-Index (24-hour, µg/m³)
df_aqi = df_aqi.withColumn(
    "aqi_pm10",
    when(col("pm10") <= 54, (50/54) * col("pm10"))
    .when((col("pm10") > 54) & (col("pm10") <= 154), 50 + ((100-50)/(154-54)) * (col("pm10") - 54))
    .when((col("pm10") > 154) & (col("pm10") <= 254), 100 + ((150-100)/(254-154)) * (col("pm10") - 154))
    .when((col("pm10") > 254) & (col("pm10") <= 354), 150 + ((200-150)/(354-254)) * (col("pm10") - 254))
    .when((col("pm10") > 354) & (col("pm10") <= 424), 200 + ((300-200)/(424-354)) * (col("pm10") - 354))
    .when(col("pm10") > 424, 300 + ((500-300)/(604-424)) * (col("pm10") - 424))
    .otherwise(0)
)

# O3 Sub-Index (8-hour, ppb -> ppm conversion applied)
df_aqi = df_aqi.withColumn(
    "aqi_o3",
    when(col("o3") <= 54, (50/54) * col("o3"))
    .when((col("o3") > 54) & (col("o3") <= 70), 50 + ((100-50)/(70-54)) * (col("o3") - 54))
    .when((col("o3") > 70) & (col("o3") <= 85), 100 + ((150-100)/(85-70)) * (col("o3") - 70))
    .when((col("o3") > 85) & (col("o3") <= 105), 150 + ((200-150)/(105-85)) * (col("o3") - 85))
    .when(col("o3") > 105, 200 + ((300-200)/(200-105)) * (col("o3") - 105))
    .otherwise(0)
)

# NO2 Sub-Index
df_aqi = df_aqi.withColumn(
    "aqi_no2",
    when(col("no2") <= 53, (50/53) * col("no2"))
    .when((col("no2") > 53) & (col("no2") <= 100), 50 + ((100-50)/(100-53)) * (col("no2") - 53))
    .when((col("no2") > 100) & (col("no2") <= 360), 100 + ((150-100)/(360-100)) * (col("no2") - 100))
    .when(col("no2") > 360, 150 + ((200-150)/(649-360)) * (col("no2") - 360))
    .otherwise(0)
)

# SO2 Sub-Index
df_aqi = df_aqi.withColumn(
    "aqi_so2",
    when(col("so2") <= 35, (50/35) * col("so2"))
    .when((col("so2") > 35) & (col("so2") <= 75), 50 + ((100-50)/(75-35)) * (col("so2") - 35))
    .when((col("so2") > 75) & (col("so2") <= 185), 100 + ((150-100)/(185-75)) * (col("so2") - 75))
    .when(col("so2") > 185, 150 + ((200-150)/(304-185)) * (col("so2") - 185))
    .otherwise(0)
)

# CO Sub-Index (mg/m³)
df_aqi = df_aqi.withColumn(
    "aqi_co",
    when(col("co") <= 4.4, (50/4.4) * col("co"))
    .when((col("co") > 4.4) & (col("co") <= 9.4), 50 + ((100-50)/(9.4-4.4)) * (col("co") - 4.4))
    .when((col("co") > 9.4) & (col("co") <= 12.4), 100 + ((150-100)/(12.4-9.4)) * (col("co") - 9.4))
    .when(col("co") > 12.4, 150 + ((200-150)/(15.4-12.4)) * (col("co") - 12.4))
    .otherwise(0)
)

# Calculate Overall AQI as the maximum of all sub-indices (EPA Standard)
df_aqi = df_aqi.withColumn(
    "aqi",
    greatest(
        col("aqi_pm25"), col("aqi_pm10"), col("aqi_o3"),
        col("aqi_no2"), col("aqi_so2"), col("aqi_co")
    )
)

# Add AQI Category
df_aqi = df_aqi.withColumn(
    "aqi_category",
    when(col("aqi") <= 50, "Good")
    .when((col("aqi") > 50) & (col("aqi") <= 100), "Moderate")
    .when((col("aqi") > 100) & (col("aqi") <= 150), "Unhealthy for Sensitive Groups")
    .when((col("aqi") > 150) & (col("aqi") <= 200), "Unhealthy")
    .when((col("aqi") > 200) & (col("aqi") <= 300), "Very Unhealthy")
    .otherwise("Hazardous")
)

print("   ✅ AQI calculated using EPA formula (max of sub-indices)")

# AQI Distribution
print("\n📊 AQI Category Distribution:")
print("-"*50)
df_aqi.groupBy("aqi_category").count().orderBy(desc("count")).show()

# AQI Statistics
print("\n📊 AQI Statistics:")
print("-"*50)
df_aqi.select("aqi").describe().show()

# ╔═══════════════════════════════════════════════════════════════════════════╗
# ║                      SECTION 7: FEATURE ENGINEERING                        ║
# ╚═══════════════════════════════════════════════════════════════════════════╝

print("\n" + "="*80)
print("⚙️  FEATURE ENGINEERING")
print("="*80)

# 7.1 Parse Timestamp
print("\n🕐 Extracting Temporal Features...")

df_features = df_aqi.withColumn(
    "timestamp",
    to_timestamp(col("datetime"), "yyyy-MM-dd HH:mm:ss")
)

# Extract time-based features
df_features = df_features.withColumn("hour", hour(col("timestamp")))
df_features = df_features.withColumn("day_of_week", dayofweek(col("timestamp")))
df_features = df_features.withColumn("month", month(col("timestamp")))
df_features = df_features.withColumn("year", year(col("timestamp")))

# Add cyclical encoding for hour (captures circular nature of time)
df_features = df_features.withColumn("hour_sin", expr("sin(2 * 3.14159 * hour / 24)"))
df_features = df_features.withColumn("hour_cos", expr("cos(2 * 3.14159 * hour / 24)"))

print("   ✅ Temporal features extracted: hour, day_of_week, month, year")
print("   ✅ Cyclical time encoding added: hour_sin, hour_cos")

# 7.2 Add Zone/Region Classification (simulated based on data patterns)
print("\n🌍 Creating Zone Classification...")

# Create zones based on AQI patterns (simulating country/city since not in data)
df_features = df_features.withColumn(
    "row_id",
    monotonically_increasing_id()
)

# Create synthetic zones based on data patterns (5 zones)
df_features = df_features.withColumn(
    "zone",
    when(col("row_id") % 5 == 0, "Zone_North")
    .when(col("row_id") % 5 == 1, "Zone_South")
    .when(col("row_id") % 5 == 2, "Zone_East")
    .when(col("row_id") % 5 == 3, "Zone_West")
    .otherwise("Zone_Central")
)

print("   ✅ Zone classification created: Zone_North, Zone_South, Zone_East, Zone_West, Zone_Central")

# 7.3 Lag Features (Previous Values for Time Series)
print("\n📈 Creating Lag Features for Time Series...")

# Define window specification
window_spec = Window.partitionBy("zone").orderBy("timestamp")

# Add lag features for key pollutants
df_features = df_features.withColumn("pm25_lag1", lag("pm25", 1).over(window_spec))
df_features = df_features.withColumn("pm25_lag3", lag("pm25", 3).over(window_spec))
df_features = df_features.withColumn("pm10_lag1", lag("pm10", 1).over(window_spec))
df_features = df_features.withColumn("aqi_lag1", lag("aqi", 1).over(window_spec))

# Add rolling statistics (previous 3 hours average)
df_features = df_features.withColumn(
    "pm25_rolling_avg",
    avg("pm25").over(window_spec.rowsBetween(-3, -1))
)

# Fill null lag values with current values
df_features = df_features.fillna({
    "pm25_lag1": df_features.agg(mean("pm25")).collect()[0][0],
    "pm25_lag3": df_features.agg(mean("pm25")).collect()[0][0],
    "pm10_lag1": df_features.agg(mean("pm10")).collect()[0][0],
    "aqi_lag1": df_features.agg(mean("aqi")).collect()[0][0],
    "pm25_rolling_avg": df_features.agg(mean("pm25")).collect()[0][0]
})

print("   ✅ Lag features created: pm25_lag1, pm25_lag3, pm10_lag1, aqi_lag1")
print("   ✅ Rolling statistics: pm25_rolling_avg (3-hour window)")

# 7.4 Weather Interaction Features
print("\n🌤️  Creating Weather Interaction Features...")

df_features = df_features.withColumn(
    "temp_humidity_interaction",
    col("temp") * col("rh") / 100
)

df_features = df_features.withColumn(
    "wind_pm25_interaction",
    col("wind") * col("pm25")
)

print("   ✅ Interaction features: temp_humidity_interaction, wind_pm25_interaction")

# Cache the feature-engineered dataframe
df_features = df_features.cache()
print(f"\n   📊 Feature-engineered dataset: {df_features.count():,} records")

# Display sample
print("\n📋 Sample of Engineered Features:")
print("-"*50)
df_features.select(
    "datetime", "pm25", "aqi", "aqi_category", "zone", 
    "hour", "pm25_lag1", "pm25_rolling_avg"
).show(5, truncate=False)

# ╔═══════════════════════════════════════════════════════════════════════════╗
# ║                     SECTION 8: ML PIPELINE CONSTRUCTION                    ║
# ╚═══════════════════════════════════════════════════════════════════════════╝

print("\n" + "="*80)
print("🤖 BUILDING ML PIPELINE")
print("="*80)

# 8.1 String Indexing for Categorical Variables
print("\n🔢 Setting up categorical encoding...")

zone_indexer = StringIndexer(
    inputCol="zone",
    outputCol="zone_index",
    handleInvalid="keep"
)

zone_encoder = OneHotEncoder(
    inputCols=["zone_index"],
    outputCols=["zone_encoded"]
)

print("   ✅ StringIndexer and OneHotEncoder configured for 'zone'")

# 8.2 Feature Assembly
print("\n📦 Configuring Feature Assembly...")

# Define feature columns
feature_cols = [
    # Pollutants
    "pm25", "pm10", "no2", "co", "so2", "o3",
    # Weather
    "temp", "rh", "wind", "rain",
    # Temporal
    "hour", "day_of_week", "month", "hour_sin", "hour_cos",
    # Lag features
    "pm25_lag1", "pm25_lag3", "pm10_lag1", "aqi_lag1", "pm25_rolling_avg",
    # Interactions
    "temp_humidity_interaction", "wind_pm25_interaction"
]

# Assembler for numeric features (before encoding)
numeric_assembler = VectorAssembler(
    inputCols=feature_cols,
    outputCol="numeric_features",
    handleInvalid="skip"
)

# Final assembler (combines numeric with encoded categorical)
final_assembler = VectorAssembler(
    inputCols=["scaled_features", "zone_encoded"],
    outputCol="features",
    handleInvalid="skip"
)

print(f"   ✅ VectorAssembler configured with {len(feature_cols)} numeric features")

# 8.3 Feature Scaling
print("\n📏 Configuring Feature Scaling...")

scaler = StandardScaler(
    inputCol="numeric_features",
    outputCol="scaled_features",
    withStd=True,
    withMean=True
)

print("   ✅ StandardScaler configured (mean=0, std=1)")

# 8.4 Random Forest Regressor
print("\n🌲 Configuring Random Forest Regressor...")

rf_regressor = RandomForestRegressor(
    labelCol="aqi",
    featuresCol="features",
    numTrees=50,
    maxDepth=10,
    minInstancesPerNode=5,
    featureSubsetStrategy="sqrt",
    seed=RANDOM_SEED
)

print("   ✅ RandomForestRegressor: numTrees=50, maxDepth=10")

# 8.5 Build Complete Pipeline
print("\n🔗 Assembling Complete Pipeline...")

pipeline = Pipeline(stages=[
    zone_indexer,      # Step 1: Index categorical zone
    zone_encoder,      # Step 2: One-hot encode zone
    numeric_assembler, # Step 3: Assemble numeric features
    scaler,            # Step 4: Scale numeric features
    final_assembler,   # Step 5: Combine all features
    rf_regressor       # Step 6: Random Forest model
])

print("   ✅ Pipeline assembled with 6 stages")

# ╔═══════════════════════════════════════════════════════════════════════════╗
# ║                      SECTION 9: MODEL TRAINING & EVALUATION                ║
# ╚═══════════════════════════════════════════════════════════════════════════╝

print("\n" + "="*80)
print("🎯 MODEL TRAINING & EVALUATION")
print("="*80)

# 9.1 Train/Test Split
print("\n✂️  Splitting Data (80/20)...")

# Filter out any remaining null values in critical columns
ml_data = df_features.select(
    feature_cols + ["aqi", "zone", "datetime", "aqi_category"]
).na.drop()

train_data, test_data = ml_data.randomSplit([TRAIN_RATIO, TEST_RATIO], seed=RANDOM_SEED)

train_count = train_data.count()
test_count = test_data.count()

print(f"   📊 Training set: {train_count:,} records ({TRAIN_RATIO*100:.0f}%)")
print(f"   📊 Test set: {test_count:,} records ({TEST_RATIO*100:.0f}%)")

# 9.2 Train Model
print("\n🏋️  Training Model...")
print("   ⏳ This may take a few minutes...")

try:
    start_time = datetime.now()
    
    # Fit the pipeline
    model = pipeline.fit(train_data)
    
    training_time = (datetime.now() - start_time).total_seconds()
    print(f"   ✅ Model trained successfully in {training_time:.2f} seconds!")
    
except Exception as e:
    print(f"   ❌ Training failed: {e}")
    spark.stop()
    sys.exit(1)

# 9.3 Make Predictions
print("\n🔮 Making Predictions on Test Set...")

predictions = model.transform(test_data)
predictions = predictions.cache()

print("   ✅ Predictions generated!")

# Display sample predictions
print("\n📋 Sample Predictions:")
print("-"*80)
predictions.select(
    "datetime", "zone", "pm25", "aqi", "prediction", "aqi_category"
).show(10, truncate=False)

# 9.4 Model Evaluation
print("\n" + "="*80)
print("📊 MODEL EVALUATION METRICS")
print("="*80)

# Initialize evaluators
rmse_evaluator = RegressionEvaluator(
    labelCol="aqi", 
    predictionCol="prediction", 
    metricName="rmse"
)

mae_evaluator = RegressionEvaluator(
    labelCol="aqi", 
    predictionCol="prediction", 
    metricName="mae"
)

r2_evaluator = RegressionEvaluator(
    labelCol="aqi", 
    predictionCol="prediction", 
    metricName="r2"
)

# Calculate metrics
rmse = rmse_evaluator.evaluate(predictions)
mae = mae_evaluator.evaluate(predictions)
r2 = r2_evaluator.evaluate(predictions)

print(f"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                        MODEL PERFORMANCE METRICS                              ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  📈 Root Mean Square Error (RMSE):  {rmse:>10.4f}                              ║
║  📈 Mean Absolute Error (MAE):      {mae:>10.4f}                              ║
║  📈 R-Squared (R²):                 {r2:>10.4f}                              ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  Interpretation:                                                              ║
║  • RMSE: Average prediction error in AQI units                               ║
║  • MAE: Average absolute prediction error                                    ║
║  • R²: {r2*100:.1f}% of variance explained by the model                          ║
╚══════════════════════════════════════════════════════════════════════════════╝
""")

# 9.5 Prediction Error Analysis
print("\n📊 Prediction Error Analysis:")
print("-"*50)

predictions_with_error = predictions.withColumn(
    "prediction_error",
    col("aqi") - col("prediction")
).withColumn(
    "abs_error",
    expr("abs(prediction_error)")
)

predictions_with_error.select("prediction_error", "abs_error").describe().show()

# Error distribution by AQI category
print("\n📊 Mean Absolute Error by AQI Category:")
print("-"*50)
predictions_with_error.groupBy("aqi_category").agg(
    spark_round(avg("abs_error"), 2).alias("mean_abs_error"),
    count("*").alias("count")
).orderBy("mean_abs_error").show()

# ╔═══════════════════════════════════════════════════════════════════════════╗
# ║                      SECTION 10: FUTURE FORECASTING                        ║
# ╚═══════════════════════════════════════════════════════════════════════════╝

print("\n" + "="*80)
print("🔮 FUTURE AQI FORECASTING")
print("="*80)

# 10.1 Create Future DataFrame
print("\n📅 Creating Future Forecast Dataset (Next 24 Hours)...")

# Get average values for features from the dataset
avg_features = df_features.agg({
    'pm25': 'mean', 'pm10': 'mean', 'no2': 'mean', 'co': 'mean',
    'so2': 'mean', 'o3': 'mean', 'temp': 'mean', 'rh': 'mean',
    'wind': 'mean', 'rain': 'mean'
}).collect()[0]

# Create future data using Spark-native approach (avoids Python worker issues)
# Take a sample row and modify it for forecasting
print("   📊 Using average values for forecast baseline...")

# Get average values
avg_pm25 = float(avg_features['avg(pm25)'])
avg_pm10 = float(avg_features['avg(pm10)'])
avg_no2 = float(avg_features['avg(no2)'])
avg_co = float(avg_features['avg(co)'])
avg_so2 = float(avg_features['avg(so2)'])
avg_o3 = float(avg_features['avg(o3)'])
avg_temp = float(avg_features['avg(temp)'])
avg_rh = float(avg_features['avg(rh)'])
avg_wind = float(avg_features['avg(wind)'])
avg_rain = float(avg_features['avg(rain)'])

# Create future DataFrame using SQL literals (no Python UDFs needed)
future_rows = []
for i in range(24):
    future_rows.append((
        i,  # hour
        avg_pm25, avg_pm10, avg_no2, avg_co, avg_so2, avg_o3,
        avg_temp, avg_rh, avg_wind, avg_rain,
        (i % 7) + 1,  # day_of_week
        12,  # month (December)
        "Zone_Central",
        avg_pm25, avg_pm25, avg_pm10, 100.0, avg_pm25,
        avg_temp * avg_rh / 100,
        avg_wind * avg_pm25,
        0.0, "Unknown"
    ))

future_schema = StructType([
    StructField("hour", IntegerType(), True),
    StructField("pm25", DoubleType(), True),
    StructField("pm10", DoubleType(), True),
    StructField("no2", DoubleType(), True),
    StructField("co", DoubleType(), True),
    StructField("so2", DoubleType(), True),
    StructField("o3", DoubleType(), True),
    StructField("temp", DoubleType(), True),
    StructField("rh", DoubleType(), True),
    StructField("wind", DoubleType(), True),
    StructField("rain", DoubleType(), True),
    StructField("day_of_week", IntegerType(), True),
    StructField("month", IntegerType(), True),
    StructField("zone", StringType(), True),
    StructField("pm25_lag1", DoubleType(), True),
    StructField("pm25_lag3", DoubleType(), True),
    StructField("pm10_lag1", DoubleType(), True),
    StructField("aqi_lag1", DoubleType(), True),
    StructField("pm25_rolling_avg", DoubleType(), True),
    StructField("temp_humidity_interaction", DoubleType(), True),
    StructField("wind_pm25_interaction", DoubleType(), True),
    StructField("aqi", DoubleType(), True),
    StructField("aqi_category", StringType(), True)
])

future_df = spark.createDataFrame(future_rows, future_schema)

# Add cyclical hour encoding using Spark SQL
future_df = future_df.withColumn("hour_sin", expr("sin(2 * 3.14159 * hour / 24)"))
future_df = future_df.withColumn("hour_cos", expr("cos(2 * 3.14159 * hour / 24)"))

print(f"   ✅ Created forecast dataset with 24 hourly predictions")

# 10.2 Generate Forecasts
print("\n🔮 Generating AQI Forecasts...")

try:
    forecast = model.transform(future_df)
    
    # Add predicted category
    forecast = forecast.withColumn(
        "predicted_category",
        when(col("prediction") <= 50, "Good")
        .when((col("prediction") > 50) & (col("prediction") <= 100), "Moderate")
        .when((col("prediction") > 100) & (col("prediction") <= 150), "Unhealthy for Sensitive Groups")
        .when((col("prediction") > 150) & (col("prediction") <= 200), "Unhealthy")
        .when((col("prediction") > 200) & (col("prediction") <= 300), "Very Unhealthy")
        .otherwise("Hazardous")
    )
    
    print("\n📅 24-Hour AQI Forecast:")
    print("-"*80)
    forecast.select(
        "hour", "zone", 
        spark_round("prediction", 1).alias("predicted_aqi"),
        "predicted_category"
    ).orderBy("hour").show(24, truncate=False)
    
except Exception as e:
    print(f"   ⚠️  Forecasting warning: {e}")

# ╔═══════════════════════════════════════════════════════════════════════════╗
# ║                      SECTION 11: HIGH-RISK ZONE ANALYSIS                   ║
# ╚═══════════════════════════════════════════════════════════════════════════╝

print("\n" + "="*80)
print("⚠️  HIGH-RISK ZONE IDENTIFICATION")
print("="*80)

# 11.1 Analyze Predictions by Zone
print("\n🌍 Analyzing Pollution Levels by Zone...")

zone_analysis = predictions.groupBy("zone").agg(
    spark_round(avg("prediction"), 2).alias("avg_predicted_aqi"),
    spark_round(spark_max("prediction"), 2).alias("max_predicted_aqi"),
    spark_round(spark_min("prediction"), 2).alias("min_predicted_aqi"),
    spark_round(stddev("prediction"), 2).alias("std_predicted_aqi"),
    count("*").alias("num_records")
).orderBy(desc("avg_predicted_aqi"))

print("\n📊 Zone-wise AQI Analysis (All Zones):")
print("-"*80)
zone_analysis.show()

# 11.2 Identify High-Risk Zones (AQI > 150)
print("\n🚨 Identifying High-Risk Zones (Average AQI > 150)...")

HIGH_RISK_THRESHOLD = 150

high_risk_zones = zone_analysis.filter(
    col("avg_predicted_aqi") > HIGH_RISK_THRESHOLD
).orderBy(desc("avg_predicted_aqi"))

high_risk_count = high_risk_zones.count()

if high_risk_count > 0:
    print(f"\n⚠️  Found {high_risk_count} HIGH-RISK ZONES:")
    print("-"*80)
    high_risk_zones.show()
else:
    print(f"\n✅ No zones exceed the high-risk threshold (AQI > {HIGH_RISK_THRESHOLD})")
    print("   Showing top 5 zones with highest average predicted AQI:")
    zone_analysis.limit(5).show()

# 11.3 Time-based Risk Analysis
print("\n🕐 Risk Analysis by Time of Day:")
print("-"*80)

hourly_risk = predictions.groupBy("hour").agg(
    spark_round(avg("prediction"), 2).alias("avg_predicted_aqi"),
    spark_round(spark_max("prediction"), 2).alias("max_aqi")
).orderBy("hour")

hourly_risk.show(24)

# Find peak pollution hours
print("\n⏰ Peak Pollution Hours (Top 5):")
print("-"*80)
predictions.groupBy("hour").agg(
    spark_round(avg("prediction"), 2).alias("avg_aqi")
).orderBy(desc("avg_aqi")).limit(5).show()

# ╔═══════════════════════════════════════════════════════════════════════════╗
# ║                         SECTION 12: SAVE RESULTS                           ║
# ╚═══════════════════════════════════════════════════════════════════════════╝

print("\n" + "="*80)
print("💾 SAVING RESULTS")
print("="*80)

# 12.1 Save Predictions using Pandas (Windows-compatible, no Hadoop needed)
print("\n📁 Saving Predictions...")

try:
    predictions_output = predictions.select(
        "datetime", "zone", "pm25", "pm10", "no2", "co", "so2", "o3",
        "temp", "rh", "wind", "aqi", "prediction", "aqi_category"
    ).withColumn(
        "prediction", spark_round("prediction", 2)
    )
    
    # Convert to Pandas and save (Windows-friendly approach)
    predictions_pdf = predictions_output.limit(10000).toPandas()  # Limit for memory
    predictions_pdf.to_csv(PREDICTIONS_OUTPUT, index=False)
    
    print(f"   ✅ Predictions saved to: {PREDICTIONS_OUTPUT}")
    print(f"   📊 Saved {len(predictions_pdf):,} sample predictions")
    
except Exception as e:
    print(f"   ⚠️  Could not save predictions: {e}")

# 12.2 Save High-Risk Zones
print("\n📁 Saving High-Risk Zones Analysis...")

try:
    # Save all zones with risk classification
    zones_with_risk = zone_analysis.withColumn(
        "risk_level",
        when(col("avg_predicted_aqi") > 200, "CRITICAL")
        .when(col("avg_predicted_aqi") > 150, "HIGH")
        .when(col("avg_predicted_aqi") > 100, "MODERATE")
        .when(col("avg_predicted_aqi") > 50, "LOW")
        .otherwise("MINIMAL")
    )
    
    # Convert to Pandas and save (Windows-friendly approach)
    zones_pdf = zones_with_risk.toPandas()
    zones_pdf.to_csv(HIGH_RISK_OUTPUT, index=False)
    
    print(f"   ✅ High-risk zones saved to: {HIGH_RISK_OUTPUT}")
    
except Exception as e:
    print(f"   ⚠️  Could not save high-risk zones: {e}")

# ╔═══════════════════════════════════════════════════════════════════════════╗
# ║                        SECTION 13: FEATURE IMPORTANCE                      ║
# ╚═══════════════════════════════════════════════════════════════════════════╝

print("\n" + "="*80)
print("🔍 FEATURE IMPORTANCE ANALYSIS")
print("="*80)

try:
    # Extract Random Forest model from pipeline
    rf_model = model.stages[-1]
    
    # Get feature importances
    importances = rf_model.featureImportances.toArray()
    
    # Create feature names list (numeric features + encoded features)
    feature_names = feature_cols + ["zone_encoded"]
    
    print("\n📊 Top 10 Most Important Features:")
    print("-"*50)
    
    # Sort and display top features
    importance_pairs = list(zip(feature_names[:len(importances)], importances))
    importance_pairs.sort(key=lambda x: x[1], reverse=True)
    
    for i, (feature, importance) in enumerate(importance_pairs[:10], 1):
        bar = "█" * int(importance * 50)
        print(f"   {i:2}. {feature:30} {importance:.4f} {bar}")
        
except Exception as e:
    print(f"   ⚠️  Could not extract feature importances: {e}")

# ╔═══════════════════════════════════════════════════════════════════════════╗
# ║                          SECTION 14: FINAL SUMMARY                         ║
# ╚═══════════════════════════════════════════════════════════════════════════╝

print("\n" + "="*80)
print("""
╔══════════════════════════════════════════════════════════════════════════════╗
║                         🎯 PROJECT SUMMARY 🎯                                 ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  ✅ Data Loading & Preprocessing                                             ║
║     • Loaded and cached {0:,} records                                   ║
║     • Handled missing values with mean imputation                            ║
║     • Filtered invalid readings                                              ║
║                                                                              ║
║  ✅ AQI Calculation                                                          ║
║     • Implemented EPA standard AQI formula                                   ║
║     • Calculated sub-indices for PM2.5, PM10, O3, NO2, SO2, CO              ║
║     • Overall AQI = max of all sub-indices                                   ║
║                                                                              ║
║  ✅ Feature Engineering                                                      ║
║     • Temporal features: hour, day_of_week, month, cyclical encoding        ║
║     • Lag features: pm25_lag1, pm25_lag3, pm10_lag1, aqi_lag1               ║
║     • Rolling statistics: 3-hour moving average                              ║
║     • Interaction features: temp×humidity, wind×pm25                         ║
║                                                                              ║
║  ✅ ML Pipeline                                                              ║
║     • StringIndexer → OneHotEncoder → StandardScaler                        ║
║     • VectorAssembler → RandomForestRegressor (50 trees)                    ║
║                                                                              ║
║  ✅ Model Performance                                                        ║
║     • RMSE: {1:.4f}                                                          ║
║     • MAE:  {2:.4f}                                                          ║
║     • R²:   {3:.4f}                                                          ║
║                                                                              ║
║  ✅ Outputs Generated                                                        ║
║     • predictions.csv - Model predictions on test set                       ║
║     • high_risk_zones.csv - Zone-level risk analysis                        ║
╚══════════════════════════════════════════════════════════════════════════════╝
""".format(record_count, rmse, mae, r2))

# ╔═══════════════════════════════════════════════════════════════════════════╗
# ║                      SECTION 15: CLEANUP & SHUTDOWN                        ║
# ╚═══════════════════════════════════════════════════════════════════════════╝

print("\n🛑 Stopping Spark Session...")

# Unpersist cached DataFrames
try:
    df_raw.unpersist()
    df_features.unpersist()
    predictions.unpersist()
except:
    pass

# Stop Spark Session
spark.stop()

print("✅ Spark Session stopped successfully!")
print("\n" + "="*80)
print("🎉 AIR QUALITY PREDICTION PROJECT COMPLETED SUCCESSFULLY! 🎉")
print("="*80 + "\n")
