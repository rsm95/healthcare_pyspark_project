import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import StorageLevel
import os

# Configure logging
log_dir = "logs"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

logging.basicConfig(
    filename=os.path.join(log_dir, 'claims_processing.log'),
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


class ClaimsProcessing:

    def __init__(self, spark,  file_path, frequent_claim_threshold=2):
        self.spark = spark
        self.file_path = file_path
        self.frequent_claim_threshold = frequent_claim_threshold
        self.df = None

    def load_data(self):
        try:
            self.df = self.spark.read.csv(self.file_path, header=True).persist(StorageLevel.MEMORY_AND_DISK)
            logger.info("Data loaded successfully")

        except Exception as e:
            logger.error(f"Error loading data: {e}")
            raise

    def basic_cleaning(self, df):
        try:
            column_names = df.columns
            for i in column_names:
                df = df.withColumn(i,
                                   when((col(i) == "\\N") | (col(i).isNull()) | (col(i) == "NA"), " ").otherwise(
                                       col(i)))

                if i not in ['insurance_start_date', 'insurance_end_date','date_of_birth']:

                    df = df.withColumn(i, regexp_replace(col(i), r'[^a-zA-Z0-9\s]', ' '))

                if i == 'health_insurance_id':
                    df = df.withColumn(i,
                                       when((col(i).startswith("HID")) & (length(col(i)) == 11), col(i))
                                       .otherwise(None))

                    # Ensure 'patients_id' starts with 'PID' and is 6 characters long
                if i == 'patientid':
                    df = df.withColumn(i,
                                       when((col(i).startswith("PID")) & (length(col(i)) == 9), col(i))
                                       .otherwise(None))

            df = df.dropDuplicates()

            return df



        except Exception as e:
            logger.error(f"Error in basic cleaning: {e}")
            raise

    def basic_cleaning1(self, df):
        try:
            # Replace special values and special characters in all columns
            column_names = df.columns
            for i in column_names:
                df = df.withColumn(i,
                                   when((col(i) == "\\N") | (col(i).isNull()) | (col(i) == "NA"), " ")
                                   .otherwise(col(i)))

                if i not in ['insurance_start_date', 'insurance_end_date']:
                    df = df.withColumn(i, regexp_replace(col(i), r'[^a-zA-Z0-9\s]', ' '))

            # Check if the required columns exist before filtering
            if 'health_insurance_id' in df.columns and 'patientid' in df.columns:
                # Filter for good data
                good_data = df.filter(
                    (col('health_insurance_id').startswith("HID")) & (length(col('health_insurance_id')) == 10) &
                    (col('patientid').startswith("PID")) & (length(col('patientid')) == 6))

                # Filter for bad data
                bad_data = df.filter(
                    ~((col('health_insurance_id').startswith("HID")) & (length(col('health_insurance_id')) == 10) &
                      (col('patientid').startswith("PID")) & (length(col('patientid')) == 6)))
            else:
                # If columns are missing, consider all data as bad data
                print("Required columns 'health_insurance_id' and/or 'patientid' are missing.")
                good_data = df  # Return an empty DataFrame as good data
                bad_data = df  # Return the entire DataFrame as bad data

            # Drop duplicate rows in both good and bad data
            good_data = good_data.dropDuplicates()
            bad_data = bad_data.dropDuplicates()

            return good_data, bad_data

        except Exception as e:
            print(f"An error occurred during basic cleaning: {str(e)}")
            return None, None
    def join_dataframes(self, df1, df2, join_column, join_type="inner"):
        try:
            if df2.count() < 50000:  # Assuming df2 is small enough for broadcasting
                df2 = broadcast(df2)

            joined_df = df1.join(df2, join_column, join_type)
            joined_df.persist(StorageLevel.MEMORY_AND_DISK)
            logger.info(f"Dataframes joined on {join_column} with {join_type} join")
            return joined_df
        except Exception as e:
            logger.error(f"Error joining dataframes on {join_column}: {e}")
            raise

    def identify_outliers(self, mean_claim, stddev_claim):
        try:
            threshold_high_2sigma = mean_claim + 2 * stddev_claim
            threshold_high_3sigma = mean_claim + 3 * stddev_claim

            outliers_2sigma = self.df.filter(col("claim_amount_settled") > threshold_high_2sigma).persist(
                StorageLevel.DISK_ONLY)
            outliers_3sigma = self.df.filter(col("claim_amount_settled") > threshold_high_3sigma).persist(
                StorageLevel.DISK_ONLY)

            logger.info("Outliers identified")
            return outliers_2sigma, outliers_3sigma
        except Exception as e:
            logger.error(f"Error identifying outliers: {e}")
            raise

    def identify_frequent_and_fraudulent_claims(self, disease, threshold_high_2sigma):
        try:
            frequent_claims = self.df.groupBy("hospital_id").count().withColumnRenamed("count", "claim_count")
            frequent_claims = frequent_claims.withColumn("frequent_claims",
                                                         when(col("claim_count") > self.frequent_claim_threshold,
                                                              "Yes").otherwise("No"))

            broadcast_frequent_claims = broadcast(frequent_claims)

            fraudulent_claims = self.df.withColumn("high_value_claim",
                                                   when(col("claim_amount_settled") > threshold_high_2sigma,
                                                        "Yes").otherwise("No"))
            fraudulent_claims = fraudulent_claims.join(broadcast_frequent_claims, on="hospital_id", how="left")
            fraudulent_claims = fraudulent_claims.filter(
                (col("high_value_claim") == "Yes") | (col("frequent_claims") == "Yes"))
            fraudulent_claims = fraudulent_claims.filter(col("DISEASE") == disease)

            fraudulent_claims = fraudulent_claims.select("hospital_id", "claim_id", "insurance_provider_id",
                                                         "claim_amount_settled", "high_value_claim", "claim_count",
                                                         "frequent_claims")

            frauds = fraudulent_claims.withColumn("fraud_detected", when(
                ((col("high_value_claim") == "Yes") & (col("frequent_claims") == "Yes")), "Yes").otherwise("No"))
            frauds = frauds.filter((col("high_value_claim") == "Yes") & (col("frequent_claims") == "Yes"))

            fraudulent_claims_count = fraudulent_claims.count()
            frauds_count = frauds.count()

            logger.info("Frequent and fraudulent claims identified")
            logger.info(f"Total Fraudulent Claims: {fraudulent_claims_count}, Total Frauds: {frauds_count}")

            fraudulent_claims.unpersist()
            frauds.unpersist()

            return fraudulent_claims, frauds
        except Exception as e:
            logger.error(f"Error identifying frequent and fraudulent claims for {disease}: {e}")
            raise

    # Rest of your methods...

    def process_disease(self, disease):
        try:
            mean_claim, stddev_claim = self.calculate_stats(disease)
            outliers_2sigma, outliers_3sigma = self.identify_outliers(mean_claim, stddev_claim)
            threshold_high_2sigma = mean_claim + 2 * stddev_claim
            threshold_high_3sigma = mean_claim + 3 * stddev_claim
            fraudulent_claims, frauds = self.identify_frequent_and_fraudulent_claims(disease, threshold_high_2sigma)

            logger.info(f"Processing completed for disease: {disease}")

            outliers_2sigma.unpersist()
            outliers_3sigma.unpersist()

            return {
                "mean_claim": mean_claim,
                "stddev_claim": stddev_claim,
                "threshold_high_2sigma": threshold_high_2sigma,
                "threshold_high_3sigma": threshold_high_3sigma,
                "fraudulent_claims": fraudulent_claims.show(truncate=False),
                "frauds": frauds.show(truncate=False)
            }
        except Exception as e:
            logger.error(f"Error processing disease {disease}: {e}")
            raise
    # def load_data(self):
    #     try:
    #         self.df = self.spark.read.csv(self.file_path, header=True).persist(
    #             StorageLevel.MEMORY_AND_DISK)
    #         logger.info("Data loaded successfully")
    #
    #     except Exception as e:
    #         logger.error(f"Error loading data: {e}")
    #         raise


    def load_csv(self, file_path, header=True):
        try:
            df = self.spark.read.csv(file_path, header=header)
            logger.info(f"Data loaded successfully from {file_path}")
            return df
        except Exception as e:
            logger.error(f"Error loading data from {file_path}: {e}")
            raise


    def select_columns(self, df, columns):
        try:
            selected_df = df.select(*columns)
            logger.info(f"Columns selected: {columns}")
            return selected_df
        except Exception as e:
            logger.error(f"Error selecting columns {columns}: {e}")
            raise


    def write_csv(self, df, file_path, mode="overwrite", header=True):
        try:
            df.write.csv(file_path, mode=mode, header=header)
            logger.info(f"Data written to {file_path}")
        except Exception as e:
            logger.error(f"Error writing data to {file_path}: {e}")
            raise



    def calculate_stats(self, disease):
        try:
            stats = self.df.filter(col("DISEASE") == disease).agg(
                mean("claim_amount_settled").alias("mean_claim"),
                stddev("claim_amount_settled").alias("stddev_claim")
            ).collect()

            if not stats:
                raise ValueError(f"No data found for disease: {disease}")

            mean_claim = stats[0]["mean_claim"]
            stddev_claim = stats[0]["stddev_claim"]

            logger.info(f"Mean and standard deviation calculated for {disease}")
            logger.info(f"Mean Claim: {mean_claim}, Standard Deviation: {stddev_claim}")

            return mean_claim, stddev_claim
        except Exception as e:
            logger.error(f"Error calculating stats for {disease}: {e}")
            raise


    def perform_cost_analysis(self,df, group_column, metrics):
        try:
            cost_analysis = df.groupBy(group_column).agg(*[sum(col(metric)).alias(f"total_{metric}") for metric in metrics])

            total_claims = cost_analysis.agg(sum(col(f"total_{metrics[-1]}")).alias("total")).collect()[0]["total"]
            cost_analysis = cost_analysis.withColumn(
                "percentage_contribution", col(f"total_{metrics[-1]}") / total_claims * 100
            )

            return cost_analysis.orderBy(col(f"total_{metrics[-1]}").desc())
        except Exception as e:
            logger.error(f"Error in cost analysis: {e}")
            raise

    def calculate_patient_cost_burden(self, df, patient_column):
        try:
            # Ensure that the columns are numeric to avoid issues in aggregation
            df = df.withColumn("outofpocketmax", col("outofpocketmax").cast("double"))
            df = df.withColumn("copay", col("copay").cast("double"))
            df = df.withColumn("deductible", col("deductible").cast("double"))
            df = df.withColumn("coinsurance", col("coinsurance").cast("double"))

            # Calculate the total cost burden components per patient
            cost_burden = df.groupBy(patient_column).agg(
                round(sum("outofpocketmax") / count("claim_id"), 2).alias("avg_out_of_pocket"),
                round(sum("copay") / count("claim_id"), 2).alias("avg_copay"),
                round(sum("deductible") / count("claim_id"), 2).alias("avg_deductible"),
                round((sum("coinsurance") / count("claim_id")) * 100, 2).alias("avg_coinsurance_percentage")

            )

            # Order by the average out-of-pocket cost burden in descending order
            return cost_burden.orderBy(col("avg_out_of_pocket").desc())

        except Exception as e:
            logger.error(f"Error in patient cost burden analysis: {e}")
            raise

    def analyze_provider_performance(self,df, provider_column, metrics):

        try:
            provider_performance = df.groupBy(provider_column).agg( avg(col(metrics[0])).alias("avg_claim_amount"),
            max(col(metrics[0])).alias("max_claim_amount"),min(col(metrics[0])).alias("min_claim_amount"),count("claim_id").alias("claim_count"))

            return provider_performance.orderBy(col("avg_claim_amount").desc(), col("claim_count").desc())

        except Exception as e:
            logger.error(f"Error in provider performance analysis: {e}")
            raise


        except Exception as e:
            logger.error(f"Error in insurance coverage analysis: {e}")
            raise

    def analyze_age_groups(self, df, age_column, metrics):
        try:
            # Create age group column
            df = df.withColumn("age_group",
                               when(col(age_column) < 18, "Child")
                               .when((col(age_column) >= 18) & (col(age_column) < 60), "Adult")
                               .otherwise("Senior"))

            # Initialize a list to hold aggregation expressions
            agg_exprs = []

            # Iterate over each metric to create the aggregation expressions
            for metric in metrics:
                agg_exprs.append(avg(col(metric)).alias(f"avg_{metric}"))
                agg_exprs.append(max(col(metric)).alias(f"max_{metric}"))
                agg_exprs.append(min(col(metric)).alias(f"min_{metric}"))

            # Perform the aggregation using the aggregation expressions list
            age_group_analysis = df.groupBy("age_group").agg(*agg_exprs)

            return age_group_analysis

        except Exception as e:
            logger.error(f"Error in age group analysis: {e}")
            raise

