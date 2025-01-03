# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC create schema if not exists bronze; 
# MAGIC create table IF NOT EXISTS bronze.control_tbl(
# MAGIC   tablename string,
# MAGIC   version int
# MAGIC ) using delta