1. Project Overview
- This project is a clinical data pipeline built using Docker.
- It processes raw healthcare data, cleans it, stores it in a data lake, and generates analytics        and visualizations.
  
2. Pipeline Flow
- The pipeline follows simple steps:
- Raw data → Data cleaning → Data lake → Analytics → Visualization
- I designed it this way to keep the process clear and modular.
- 
3. Data Cleaning
- I faced issues with messy date formats. Some dates were not parsing correctly.
- I solved this using parser.parse() with different formats.
- I also implemented per-patient trend analysis (improving vs worsening).
- This compares current values with previous records.
- I created visualizations after transformation, though some plots needed debugging

4. Data Lake Design
- I stored data in three layers: raw, cleaned, and analytical.
- I created a manifest file to store metadata like:
- file name
- file path
- number of rows
- number of null values
- This helps understand the data without opening the file.

5. Anomaly Detection
I defined anomalies such as:
- age < 0 or age > 120
- invalid lab values
- incorrect cases like male pregnancy
- unrealistic time gaps in data
This helped identify data quality issues.

6. Genomics Filtering
I filtered reliable data based on:
- high quality scores
- sufficient test coverage
- PASS status
- meaningful mutations only
I removed weak, noisy, or incomplete data.

7. Assumptions
I made some assumptions:
- removed or handled missing values
- used only PASS records
- ignored extreme outliers
- focused on important mutations
These were needed because some details were not clearly defined.

8. Improvements (Production)
With more time, I would:
- use cloud storage like Amazon S3
- automate workflows using Apache Airflow
- use Apache Spark for large-scale processing
- integrate tools like Databricks
- build real-time pipelines and monitoring systems

9. Final (important – speak this confidently)
    - I have basic knowledge of big data technologies and AWS.
    - I have worked on a few real-time projects and I am a fast learner.
    - I am very interested in working on real-world data problems and growing with a good team.
