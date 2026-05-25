# Flight Delay Analysis — Big Data Project
**Roma Tre University · Big Data Course · 2024-2025**

## Dataset
2024 Flight Delay Dataset from Kaggle (~7M records, 35 columns).

Download from: https://www.kaggle.com/datasets/hrishitpatil/flight-data-2024
Place it at: `data/raw/flight_data_2024.csv`

## Technologies used
- Apache Spark SQL (DataFrame API)
- Apache Spark Core (RDD API)
- Apache Hive 4.1.0 (via Docker)

## Analyses implemented
| Analysis | Spark SQL | Spark Core | Hive |
|----------|-----------|------------|------|
| 3.1 Airline statistics | ✅ | ✅ | ✅ |
| 3.2 Delay report | ✅ | ✅ | ✅ |
| 3.3 Anomalous delay ranking | — | — | ✅ |

## Project structure