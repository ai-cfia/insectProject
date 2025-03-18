# Databricks notebook source
# MAGIC %pip install -U setuptools

# COMMAND ----------

!python -V

# COMMAND ----------

# MAGIC %pip install dataclasses==0.6
# MAGIC %pip install future==0.18.2
# MAGIC %pip install numpy==1.21.1 
# MAGIC #1.19.4
# MAGIC %pip install Pillow==8.0.1
# MAGIC %pip install torch==1.7.1
# MAGIC %pip install torchvision==0.8.2
# MAGIC %pip install typing-extensions==3.7.4.3
# MAGIC %pip install urllib3==1.26.2
# MAGIC %pip install pandas==1.1.4
# MAGIC %pip install python-crontab==2.5.1
# MAGIC %pip install python-dateutil==2.8.1
# MAGIC %pip install pytz==2020.4
# MAGIC %pip install reverse-geocoder==1.5.1
# MAGIC %pip install scipy==1.5.4
# MAGIC %pip install six==1.15.0
# MAGIC %pip install geopy==2.1.0

# COMMAND ----------

import geopy

# COMMAND ----------

geopy.__version__

# COMMAND ----------

