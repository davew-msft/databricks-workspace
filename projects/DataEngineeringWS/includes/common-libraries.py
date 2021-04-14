# Databricks notebook source
dbutils.library.installPyPI("torch")
dbutils.library.installPyPI("azureml-sdk", extras="databricks")
