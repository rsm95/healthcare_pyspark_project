import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from utils.utilsfile import ClaimsProcessing
if __name__ == "__main__":
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    schema1 = StructType([
        StructField("patientid", StringType(), True),
        StructField("patient_name", StringType(), True),
        StructField("date_of_birth", StringType(), True),
        StructField("p_age", IntegerType(), True),
        StructField("gender", StringType(), True),
        StructField("patient_address", StringType(), True),
        StructField("health_insurance_id", StringType(), True),
        StructField("hospital_id", StringType(), True),
        StructField("Hospital_address", StringType(), True),
        StructField("physician_id", StringType(), True),
        StructField("Speciality", StringType(), True),
        StructField("INSURANCE_provider_name", StringType(), True),
        StructField("insurance_provider_id", StringType(), True),
        StructField("DISEASE", StringType(), True),
        StructField("SUB_DISEASE", StringType(), True),
        StructField("DISEASE_CODE", StringType(), True),
        StructField("DIAGNOSIS/TREATMENT", StringType(), True),
        StructField("medicine_names", StringType(), True),
        StructField("medical_bill_AMOUNT", IntegerType(), True),
        StructField("hospital_bill_AMOUNT", IntegerType(), True),
        StructField("INSURANCE_PLAN", StringType(), True),
        StructField("INSURANCE_PREMIUM", IntegerType(), True),
        StructField("insurance_start_date", StringType(), True),
        StructField("insurance_end_date", StringType(), True),
        StructField("claim_id", StringType(), True),
        StructField("claim_amount", IntegerType(), True),
        StructField("deductible", IntegerType(), True),
        StructField("copay", IntegerType(), True),
        StructField("COINSURANCE", DoubleType(), True),
        StructField("outofpocketmax", DoubleType(), True),
        StructField("claim_amount_settled", DoubleType(), True),
        StructField("payment_status", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("payment_id", StringType(), True)
    ])

    file_path = r"D:\FINALPROJECTS\New folder\merged_data\merged_data_final - Copy.csv"

    utils = ClaimsProcessing(spark, schema1, file_path)
    utils.load_data()

    diseases = ["Cancer", "Diabetes", "Chronic Kidney Disease", "Neurological Disorders", "Cardiovascular Diseases", "Chronic Respiratory Diseases"]

    for disease in diseases:
        result = utils.process_disease(disease)
        print(f"Results for {disease}:")
        print(result)
        print("########################################################################")
