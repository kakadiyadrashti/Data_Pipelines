# Data Pipelines – Big Data Engineering Project

## Overview
This project presents a production-ready ELT (Extract, Load, Transform) data pipeline built for analysing Airbnb listings and Census data for Sydney. The pipeline integrates multiple real-world datasets and transforms them into business-ready insights using a modern data engineering stack.

The system is designed using a Medallion Architecture (Bronze → Silver → Gold) to ensure scalability, reliability, and high data quality.

---

## Tech Stack
- Apache Airflow – Workflow orchestration  
- PostgreSQL (GCP) – Data storage  
- dbt Cloud – Data transformation & modeling  
- SQL & Python – Data processing  
- Google Cloud Platform (GCP) – Infrastructure  

---

## Architecture

### 🥉 Bronze Layer
- Stores raw data from Airbnb and Census datasets  
- No transformations applied  
- Maintains data integrity for auditing  

### 🥈 Silver Layer
- Data cleaning and transformation  
- Handles missing values, duplicates, and inconsistencies  
- Standardises data for analysis  
- Splits Airbnb data into entities (hosts, listings, suburbs, LGAs)  

### 🥇 Gold Layer
- Implements Star Schema  
- Contains fact tables (revenue, bookings, availability)  
- Contains dimension tables (hosts, properties, locations)  
- Uses SCD Type II for historical tracking  
- Supports data marts for business analysis  

---

## Pipeline Workflow
1. Upload data to GCP Storage  
2. Airflow DAG extracts and loads data into PostgreSQL (Bronze layer)  
3. dbt transforms data into Silver and Gold layers  
4. Data marts are created for reporting and analysis  

---

## Key Analysis
- Revenue comparison between top and bottom performing LGAs  
- Relationship between median age and revenue  
- Best-performing property types and listing configurations  
- Host distribution across LGAs  
- Airbnb income vs mortgage repayment analysis  

---

## Key Features
- End-to-end automated pipeline using Airflow  
- Scalable data warehouse design  
- Historical tracking using SCD Type II  
- Data quality validation and cleaning  
- Modular and production-ready architecture  

---

## Challenges & Solutions
- Data inconsistencies → resolved using cleaning and validation  
- Airflow pipeline failures → handled using retries and dependencies  
- Connection issues → fixed through configuration updates  
- Performance issues → improved using query optimisation  

---

## Project Structure
Data_Pipelines/
│
├── models/
├── macros/
├── snapshots/
├── bronze_ingest_jsonb.py
├── dbt_project.yml
├── part4.sql
├── README.md

---

## Conclusion
This project demonstrates how raw data can be transformed into actionable insights using a structured and automated data pipeline. The combination of Airflow, dbt, and PostgreSQL ensures a scalable and reliable solution for real-world analytics.

---

## Author
Drashti Kakadiya  
