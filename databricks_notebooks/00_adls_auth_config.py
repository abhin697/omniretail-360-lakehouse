# Databricks notebook source
# DBTITLE 1,Connection Test
# 00_adls_auth_config: Central ADLS Gen2 OAuth 2.0 & Lakehouse Paths
storage_account = "adlsomniretail"

client_id = dbutils.secrets.get(scope="omniretail-scope", key="sp-client-id")
tenant_id = dbutils.secrets.get(scope="omniretail-scope", key="sp-tenant-id")
client_secret = dbutils.secrets.get(scope="omniretail-scope", key="sp-client-secret")

spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", client_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", client_secret)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# Medallion Container Base Paths
LANDING_PATH = f"abfss://landing@{storage_account}.dfs.core.windows.net"
BRONZE_PATH  = f"abfss://bronze@{storage_account}.dfs.core.windows.net"
SILVER_PATH  = f"abfss://silver@{storage_account}.dfs.core.windows.net"
GOLD_PATH    = f"abfss://gold@{storage_account}.dfs.core.windows.net"

print("ADLS Gen2 OAuth & Medallion paths initialized successfully.")