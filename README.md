## PySpark Data Processing
[![CI](https://github.com/aoaow/pyspark_data_processing/actions/workflows/cicd.yml/badge.svg)](https://github.com/aoaow/pyspark_data_processing/actions/workflows/cicd.yml)

---

## **Project Description**

This project utilizes **PySpark** to perform data processing on a large dataset related to serious injury outcome indicators from 2000 to 2022. The script includes:

- Reading and preprocessing a CSV dataset.
- Performing data transformations using PySpark DataFrame operations.
- Executing a Spark SQL query to analyze the data.
- Logging outputs to a markdown file for reporting.

---

## **Dataset Information**

The dataset used is `serious-injury-outcome-indicators-2000-2022.csv`, which contains records of injury outcomes with various attributes such as age, severity, cause, and period.

---

## **Prerequisites**

- **Docker**: Ensure Docker is installed and running.
- **Visual Studio Code**: With the **Dev Containers** extension installed.
- **Python 3.9 or higher** (if not using Docker).
- **Java Development Kit (JDK)**: Required for Spark (if not using Docker).

---


## **Running the Script**

1. **Navigate to the Project Directory**:

   ```bash
   cd pyspark_data_processing
   ```

2. **Run the PySpark Script**:

   - Using `spark-submit`:

     ```bash
     spark-submit main.py
     ```

   - Or using Python:

     ```bash
     python main.py
     ```

3. **View the Output**:

   - The script generates a markdown file named `pyspark_output.md` containing the logged outputs of data loading, transformations, and SQL query results.
   - Open `pyspark_output.md` to review the outputs.

---

## **Data Processing Overview**

### **Data Loading**

- The script reads the CSV dataset into a PySpark DataFrame with a predefined schema for consistent data types.
- A temporary view named `injuryOutcome` is created from the DataFrame to facilitate Spark SQL queries.

### **Data Transformation**

- **Extracting Year**:

  - A new column `year` is added by extracting the year from the `Period` column.

- **Flagging Records Before 2010**:

  - A new column `before_2010` is created, where:
    - `1` indicates the record is from before 2010.
    - `0` indicates the record is from 2010 or later.

### **Spark SQL Query**

- The script executes a Spark SQL query to analyze injury cases by severity:

  ```sql
  SELECT Severity, COUNT(*) AS total_cases
  FROM injuryOutcome
  GROUP BY Severity
  ORDER BY total_cases DESC
  ```

- **Purpose**:

  - To investigate the number of cases in each severity category.

---

## **Output**

- **Console Output**:

  - The script prints the results of data transformations and the SQL query to the console.

- **Markdown Report** (`pyspark_output.md`):

  - Logs each operation with headers, including:
    - Operation performed.
    - SQL query executed (if applicable).
    - Truncated output of the results in a markdown table format.
  