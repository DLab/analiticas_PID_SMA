
import glob
import os
import pandas as pd
import numpy as np

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, TimestampType


# creación entorno de trabajo
spark = SparkSession.builder\
        .appName("limpieza_y_validacion_de_datos")\
        .config('spark.driver.memory', '12g')\
        .getOrCreate()

path_actual = os.getcwd()
parquetList = glob.glob(path_actual + "/" + "DATA" + "/*/*.parquet")  # lista de parquets



# lectura de archivos en pyspark
def readParquet(parquetDF):
    """
    Lectura de archivos parquet. Retorna un dataframe pyspark
    """
    df_parquet = spark.read.parquet(parquetDF)  # reemplazar por el ingreso del DF
    df_parquet = df_parquet.drop("DeviceId", "Sistema", "Timestamp", "TimestampUTC", "IdVerificacion")
    df_parquet = df_parquet.withColumnRenamed('DispositivoId', 'dispositivoId')\
                           .withColumnRenamed('Nombre', 'parametro')\
                           .withColumnRenamed('Valor', 'valor')\
                           .withColumnRenamed('Unidad', 'unidad')\
                           .withColumnRenamed('EstampaTiempo', 'fecha')

    # df_pandas = df_parquet.toPandas()
    
    return df_parquet


def typeDF(parquetDF, typeData):
    df = None

    if typeData == "Crudo":
        # IMPORTANTE CORREGIR VALORES EN DB
        df = parquetDF.filter(F.col("Crudo") == "DC ").drop("Calibraciones", "Validados")
    elif typeData == "Calibraciones":
        df = parquetDF.filter(F.col("Calibraciones") == "DP").drop("Crudo", "Validados")
    elif typeData == "Validados":
        df = parquetDF.filter(F.col("Validados") == "DV").drop("Crudo", "Calibraciones")
    else:
        print("typeData invalido")

    df_pandas = df.toPandas()

    return df_pandas


def typeDF_parquet(parquetDF, typeData):
    """
    Filtra por tipo de dato (crudo, calibrado, validado). Retorna un dataframe en pyspark
    """
    df = None

    if typeData == "Crudo":
        # IMPORTANTE CORREGIR VALORES EN DB
        df = parquetDF.filter(F.col("Crudo") == "DC ").drop("Calibraciones", "Validados")
    elif typeData == "Calibraciones":
        df = parquetDF.filter(F.col("Calibraciones") == "DP").drop("Crudo", "Validados")
    elif typeData == "Validados":
        df = parquetDF.filter(F.col("Validados") == "DV").drop("Crudo", "Calibraciones")
    else:
        print("typeData invalido")

    return df


def exploreValues(df, colList, attr=None):
    res, result = {}, None
    
    if attr is None:
        for column in colList:
            if column in ["valor", "fecha"]: pass
            else:
                res[column] = df[column].unique()
                result   = res
    else:
        try:
            result = df[column].unique()
        except:
            res[attr] = str(attr) + " doesn't exists"
            result    = res

    return result


def exploreValues_parquet(df, colList, attr=None):
    res, result = {}, None
    
    if attr is None:
        for column in colList:
            if column in ["valor", "fecha"]: pass
            else:
                tmpList         = df.select(column).distinct()
                distinctValList = [row[column] for row in tmpList.collect()]

                res[column] = distinctValList
                result   = res
    else:
        try:
            tmpList         = df.select(attr).distinct()
            distinctValList = [row[attr] for row in tmpList.collect()]

            result = distinctValList
        except:
            res[attr] = str(attr) + "doesn't exists"
            result    = res

    return result

def cleanDF(df):
    df = df.dropna()
    df = df.drop_duplicates()

    for cols in ['Crudo', 'Calibraciones', 'Validados']:
        if cols not in df.columns:
            df[cols] = np.nan

    return df



def cleanDF_parquet(parquetDF):
    parquetDF = parquetDF.na.drop()
    parquetDF = parquetDF.dropDuplicates()

    for cols in ['Crudo', 'Calibraciones', 'Validados']:
        if cols not in parquetDF.columns:
            parquetDF = parquetDF.withColumn(cols, F.lit(None).cast(FloatType()))

    # print("parquetDF.count()", parquetDF.count())

    return parquetDF


def cleanDFv2_parquet(parquetDF):

    schema = StructType([
        StructField("UfId", IntegerType(), True),
        StructField("ProcesoId", IntegerType(), True),
        StructField("dispositivoId", IntegerType(), True),
        StructField("parametro", StringType(), True),
        StructField("valor", FloatType(), True),
        StructField("unidad", StringType(), True),
        StructField("fecha", TimestampType(), True),
        StructField("Crudo", StringType(), True),
        StructField("Calibraciones", StringType(), True),
        StructField("Validados", StringType(), True)
    ])

    finalDF = spark.createDataFrame([], schema)

    listProcId = exploreValues_parquet(parquetDF, parquetDF.columns, attr='ProcesoId')
    
    for procId in listProcId:
        # print(procId)
        dfProcId   = parquetDF.filter(F.col("ProcesoId") == procId)
        listParams = exploreValues_parquet(dfProcId, dfProcId.columns, attr='parametro')
        # print(listParams)
        
        for param in listParams:
            dfParam    = dfProcId.filter(F.col("parametro") == param)
            listDispId = exploreValues_parquet(dfParam, dfParam.columns, attr='dispositivoId')
            # print(procId, param, listDispId)

            if len(listDispId) == 1:
                dfParam = dfParam.na.drop()
                dfParam = dfParam.dropDuplicates()

                for cols in ['Crudo', 'Calibraciones', 'Validados']:
                    if cols not in dfParam.columns:
                        dfParam = dfParam.withColumn(cols, F.lit(None).cast(FloatType()))

                dfParam = dfParam.select("UfId", "ProcesoId", "dispositivoId", "parametro", "valor", "unidad", "fecha", "Crudo", "Calibraciones", "Validados")

                finalDF = finalDF.union(dfParam)
            # else:
                # AIUDA
                # No sé si las mediciones de dos o más dispositivos son al mismo tiempo, por lo que no estoy segura de cómo promediar

    # print("finalDF.count()", finalDF.count())

    return finalDF


def returnDF(df, typeData):
    tmpDF      = readParquet(parquet)  # reemplazar por el ingreso del DF
    typeFilter = typeDF(tmpDF, typeData)
    cleanedDF  = cleanDF(typeFilter)

    return cleanedDF


def returnDF_parquet(parquet, typeData):
    tmpDF      = readParquet(parquet)  # reemplazar por el ingreso del DF
    typeFilter = typeDF_parquet(tmpDF, typeData)
    cleanedDF  = cleanDF_parquet(typeFilter)

    return cleanedDF




# for parquet in parquetList:
#     print(returnDF_parquet(parquet, "Validados").show())

"""
             Si se requiere, escribir función para concatenar otra vez los dataframes
"""


# Devuelve el dataframe "crudo" de pandas para los datos validados
returnDF(parquetList[0], "Validados")

# Devuelve el dataframe "crudo" de pyspark para los datos validados
returnDF_parquet(parquetList[0], "Validados")


spark.stop()


