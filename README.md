# Zach-Data-Engineering-Bootcamp
This repository contains the homework given by Zach in it's bootcamp

## Dimension Modeling - Week 1
This script manages actors' data over time by implementing Slowly Changing Dimensions (SCD) in a database. It creates custom types and tables to store detailed actor and film information, aggregates and classifies actor data yearly, and tracks changes in actor attributes (e.g., quality, activity status). Historical data is stored in an SCD table by identifying unchanged, new, and changed records, ensuring accurate and incremental updates to maintain a comprehensive record of actor performance and activity over time.
