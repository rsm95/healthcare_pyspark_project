from pyspark.sql import SparkSession
from utils.abc import ClaimsProcessing
from pyspark.sql.functions import *
from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DoubleType


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("ClaimsProcessing") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.executor.memory", "6g") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.autoBroadcastJoinThreshold", "50MB") \
        .config("spark.sql.adaptive.join.enabled", "true") \
        .getOrCreate()

    file_path = r"D:\FINALPROJECTS\New folder\merged_data\merged_data_final - Copy.csv"

    utils = ClaimsProcessing(spark, file_path)

    print("################## INSURANCE PROVIDER AND PLAN ##################")

    df1 = utils.load_csv(r"D:\FINALPROJECTS\New folder\BLUECROSS_OSCARHEALT_AETNA_CIGNA(INSURANCE_PROVIDERS).csv")

    good_df1, bad_df1 = utils.basic_cleaning1(df1)

    utils.write_csv(good_df1, r"D:\FINALPROJECTS\GOOD_BAD_DATA\GOOD_DATA\insurance_provider_good_data.csv")

    utils.write_csv(bad_df1, r"D:\FINALPROJECTS\GOOD_BAD_DATA\BAD_DATA\insurance_provider_bad_data.csv")

    print("GOOD Records:", good_df1.count())

    print("BAD Records:", bad_df1.count())



    print("################## PATIENT INFO ##################")

    df2 = utils.load_csv(r"D:\FINALPROJECTS\New folder\CERNER_CUREMD_PRACTO(PATIENT_INFO).csv")

    df21 = utils.select_columns(df2,["patientid", "patient_name", "date_of_birth", "p_age", "gender", "patient_address","city", "contact_number"])

    good_df21, bad_df21 = utils.basic_cleaning1(df21)

    utils.write_csv(good_df21, r"D:\FINALPROJECTS\GOOD_BAD_DATA\GOOD_DATA\PATIENT_INFO_good_data.csv")

    utils.write_csv(bad_df21, r"D:\FINALPROJECTS\GOOD_BAD_DATA\BAD_DATA\PATIENT_INFO_bad_data.csv")

    print("GOOD Records:", good_df21.count())

    print("BAD Records:", bad_df21.count())


    print("################## CLAIM DATA ##################")

    df3 = utils.load_csv(r"D:\FINALPROJECTS\New folder\CIGNA(INSURANCE_CLAIM_DATA).csv")

    good_df3, bad_df3 = utils.basic_cleaning1(df3)


    utils.write_csv(good_df3, r"D:\FINALPROJECTS\GOOD_BAD_DATA\GOOD_DATA\CLAIMS_good_data.csv")

    utils.write_csv(bad_df3, r"D:\FINALPROJECTS\GOOD_BAD_DATA\BAD_DATA\CLAIMS_bad_data.csv")

    print("Records:", good_df3.count())



    print("################## PAYMENT INFO ##################")

    df4 = utils.load_csv(r"D:\FINALPROJECTS\New folder\CREDIT_UPI_PAYZAPP(PAYMENT_INFO).csv")

    good_df4, bad_df4 = utils.basic_cleaning1(df4)

    utils.write_csv(good_df4, r"D:\FINALPROJECTS\GOOD_BAD_DATA\GOOD_DATA\PAYMENT_INFO_good_data.csv")

    utils.write_csv(bad_df4, r"D:\FINALPROJECTS\GOOD_BAD_DATA\BAD_DATA\PAYMENT_INFO_bad_data.csv")

    print("Records:", good_df4.count())



    print("################## DISEASE INFO ##################")

    df5 = utils.load_csv(r"D:\FINALPROJECTS\New folder\HEALTHTAP_HEALTHVAULT(patient_information).csv")

    good_df5, bad_df5 = utils.basic_cleaning1(df5)


    utils.write_csv(good_df5, r"D:\FINALPROJECTS\GOOD_BAD_DATA\GOOD_DATA\DISEASE_INFO_good_data.csv")

    utils.write_csv(bad_df5, r"D:\FINALPROJECTS\GOOD_BAD_DATA\BAD_DATA\DISEASE_INFO_bad_data.csv")

    print("Records:", good_df5.count())



    print("################## MEDICINE INFO ##################")

    df6 = utils.load_csv(r"D:\FINALPROJECTS\New folder\MEDISAFE_CAREZONE_GOODRX_NETMEDS(MEDICINE_BILLS).csv")


    df6 = utils.select_columns(df6, ["patientid", "hospital_id", "physician_id", "DISEASE_CODE", "medicine_names","medical_bill_AMOUNT"])

    good_df6, bad_df6 = utils.basic_cleaning1(df6)


    utils.write_csv(good_df6, r"D:\FINALPROJECTS\GOOD_BAD_DATA\GOOD_DATA\MEDICINE_INFO_good_data.csv")

    utils.write_csv(bad_df6, r"D:\FINALPROJECTS\GOOD_BAD_DATA\BAD_DATA\MEDICINE_INFO_bad_data.csv")

    print("Records:", good_df6.count())


    print("################## PROVIDER INFO ##################")

    df7 = utils.load_csv(r"D:\FINALPROJECTS\New folder\HOSPITALS.csv")

    df7 = utils.select_columns(df7, ["hospital_id", "Hospital_address", "physician_id", "Speciality", "INSURANCE_provider_name", "insurance_provider_id"])

    good_df7, bad_df7 = utils.basic_cleaning1(df7)


    utils.write_csv(good_df7, r"D:\FINALPROJECTS\GOOD_BAD_DATA\GOOD_DATA\PROVIDER_INFO_good_data.csv")

    utils.write_csv(bad_df7, r"D:\FINALPROJECTS\GOOD_BAD_DATA\BAD_DATA\PROVIDER_INFO_bad_data.csv")

    print("Records:", good_df7.count())

    print("################## JOIN 1 ##################")

    print("PATIENT_INFO,DISEASE_INFO,MEDICINE_INFO")

    df8 = utils.join_dataframes(good_df21, good_df5, "patientid")

    df8 = utils.join_dataframes(df8, good_df6, "patientid")

    df8.show()

    print("Count of above dataframe is:", df8.count())

    print("################## JOIN 2 ##################")
    print("HOSPITALS_INFO,CLAIM_INFO,PAYMENT_INFO")
    df9 = utils.join_dataframes(good_df3, good_df4, "hospital_id")
    df9 = utils.join_dataframes(df9, good_df7, "hospital_id")
    df9.show()
    print("Count of above dataframe is:", df9.count())

    print("################## COMBINED_TABLES ##################")
    df10 = utils.join_dataframes(df8, df9, "hospital_id")
    df10 = utils.join_dataframes(df10, good_df1, "health_insurance_id")
    df10.show()
    print("Count of above dataframe is:", df10.count())

    print("################## SELECTED COLUMNS FROM TABLES ##################")
    df11 = df10.select("patientid", df21.patient_name, df21.date_of_birth, df21.p_age, df21.gender, df21.patient_address,df3.health_insurance_id, df3.hospital_id, "Hospital_address", df7.physician_id, df7.Speciality,df7.INSURANCE_provider_name, df7.insurance_provider_id, df5.DISEASE, df5.SUB_DISEASE, df5.DISEASE_CODE,df5["DIAGNOSIS/TREATMENT"], df6.medicine_names, df6.medical_bill_AMOUNT, "hospital_bill_AMOUNT","INSURANCE_PLAN", "INSURANCE_PREMIUM", "insurance_start_date", "insurance_end_date", df3.claim_id,df3.claim_amount, df3.deductible, df3.copay, df3.COINSURANCE, df3.outofpocketmax, df3.claim_amount_settled,"payment_status", "payment_method", "payment_id"
    )

    utils.write_csv(df11,r"D:\FINALPROJECTS\GOOD_BAD_DATA\GOOD_DATA\FINAL_MERGED.csv")

    print("File successfully merged and saved to local system")

    final_df = spark.read.csv(r"D:\FINALPROJECTS\New folder\merged_data\merged_data_final - Copy.csv",header=True,inferSchema=True)

    final_df.cache()

    final_df.show()

    print("################################ 1.DETECT FRAUD DISEASE WISE ################################")
    utils.load_data()

    diseases = ["Cancer", "Diabetes", "Chronic Kidney Disease", "Neurological Disorders", "Cardiovascular Diseases",
                "Chronic Respiratory Diseases"]

    for disease in diseases:
        result = utils.process_disease(disease)
        print(f"Results for {disease}:")
        print(result)
        print("########################################################################")


    print("################################ 2.DETECT_PATIENT_COST_ANALYSIS ################################")


    metrics = ["medical_bill_AMOUNT", "hospital_bill_AMOUNT", "copay", "coinsurance", "deductible", "claim_amount"]

    cost_analysis_result = utils.perform_cost_analysis(final_df, "INSURANCE_PLAN", metrics)

    cost_analysis_result.show()


    print("################################ 3.DETECT_PATIENT_COST_BURDEN  ################################")

    patient_cost_burden_result = utils.calculate_patient_cost_burden(final_df, "patientid")

    patient_cost_burden_result.show()




    print("################################ 4.ANALYSE_PROVIDER_PERFORMANCE ################################")


    metrics = ["claim_amount_settled"]

    provider_performance_result = utils.analyze_provider_performance(final_df, "INSURANCE_provider_name", metrics)

    provider_performance_result.show()


    print("################################ 5.AGE_GROPWISE_COST_ANALYSIS ################################")


    metrics = ["claim_amount"]

    age_group_analysis_result = utils.analyze_age_groups(final_df, "p_age", metrics)

    age_group_analysis_result.show()
