Real-Time Weather Monitoring Platform
The Goal: Move weather data from API to Dashboard in < 30 seconds.

Architecture (The Flow)
Source: Python Producer (Open-Meteo API)
Transport: Azure Event Hubs (Streaming)
Storage: ADLS Gen2 (Bronze/Raw)
Processing: Databricks DLT (Medallion Architecture)
BI: Power BI / Databricks SQL (Gold Layer)

Technical Highlights
Silver Layer: Real-time flattening of JSON payloads.
Gold (Trends): 1-hour rolling window aggregates with a 15-minute slide for smooth time-series charts.
Gold (Snapshot): SCD Type 1 upserts (apply_changes) to keep the map lightweight (1 row per city).
Optimization: Z-Ordering on city_name and Watermarking to handle late-arriving data.
Enrichment: Automated mapping of WMO weather codes to human-readable text.

Why this is in my Portfolio:
Efficiency: I didn't just dump data; I pre-aggregated it to save on compute costs.
Scalability: The pipeline uses Terraform for infrastructure and DLT for auto-scaling.
Real-World Logic: It handles out-of-order data and avoids "data bloat" in the presentation layer.
