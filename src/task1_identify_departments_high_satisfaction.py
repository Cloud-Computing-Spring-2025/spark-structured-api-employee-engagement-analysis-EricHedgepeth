# task1_identify_departments_high_satisfaction.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, round as spark_round

def initialize_spark(app_name="Task1_Identify_Departments"):
    """
    Initialize and return a SparkSession.
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()
    return spark

def load_data(spark, file_path):
    """
    Load the employee data from a CSV file into a Spark DataFrame.

    Parameters:
        spark (SparkSession): The SparkSession object.
        file_path (str): Path to the employee_data.csv file.

    Returns:
        DataFrame: Spark DataFrame containing employee data.
    """
    schema = "EmployeeID INT, Department STRING, JobTitle STRING, SatisfactionRating INT, EngagementLevel STRING, ReportsConcerns BOOLEAN, ProvidedSuggestions BOOLEAN"
    
    df = spark.read.csv(file_path, header=True, schema=schema)
    return df

def identify_departments_high_satisfaction(df):
    """
    Identify departments with more than 50% of employees having a Satisfaction Rating > 4 and Engagement Level 'High'.
    """

    # Step 1: Filter employees who meet the criteria
    high_satisfaction_df = df.filter((col("SatisfactionRating") > 4) & (col("EngagementLevel") == "High"))

    # Step 2: Count total employees per department
    total_employees_per_department = df.groupBy("Department").agg(count("EmployeeID").alias("TotalEmployees"))

    # Step 3: Count high-satisfaction employees per department
    high_satisfaction_count_per_department = high_satisfaction_df.groupBy("Department").agg(count("EmployeeID").alias("HighSatEngagedEmployees"))

    # Debug: Print counts before percentage calculation
    print("=== High Satisfaction Employees Per Department ===")
    high_satisfaction_count_per_department.show()

    print("=== Total Employees Per Department ===")
    total_employees_per_department.show()

    # Step 4: Calculate percentage per department
    department_percentage_df = high_satisfaction_count_per_department.join(
        total_employees_per_department, "Department"
    ).withColumn("Percentage", spark_round((col("HighSatEngagedEmployees") / col("TotalEmployees")) * 100, 2))

    # Debug: Print before filtering
    print("=== Department Satisfaction Percentage BEFORE FILTER ===")
    department_percentage_df.show()

    # Step 5: Filter departments where percentage exceeds 50%
    result_df = department_percentage_df.filter(col("Percentage") > 50).select("Department", "Percentage")

    # Debug: Print final output
    print("=== Departments Meeting 50% Threshold ===")
    result_df.show()

    return result_df

    

    

def write_output(result_df, output_path):
    """
    Write the result DataFrame to a CSV file.

    Parameters:
        result_df (DataFrame): Spark DataFrame containing the result.
        output_path (str): Path to save the output CSV file.

    Returns:
        None
    """
    result_df.coalesce(1).write.csv(output_path, header=True, mode='overwrite')
    #result_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)

def main():
    """
    Main function to execute Task 1.
    """
    # Initialize Spark
    spark = initialize_spark()
    
    # Define file paths
    input_file = "/workspaces/spark-structured-api-employee-engagement-analysis-EricHedgepeth/input/employee_data.csv"
    output_file = "/workspaces/spark-structured-api-employee-engagement-analysis-EricHedgepeth/outputs/departments_high_satisfaction.csv"
    
    # Load data
    df = load_data(spark, input_file)
    
    # Perform Task 1
    result_df = identify_departments_high_satisfaction(df)
    
    # Write the result to CSV
    write_output(result_df, output_file)
    
    # Stop Spark Session
    spark.stop()

if __name__ == "__main__":
    main()
