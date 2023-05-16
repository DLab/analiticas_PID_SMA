
import glob
import os

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import FloatType


# creación entorno de trabajo
spark = SparkSession.builder\
        .appName("limpieza_y_validacion_de_datos")\
        .config('spark.driver.memory', '12g')\
        .getOrCreate()


def readDataframe(parquetDF):
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
    
    return df_parquet


def typeDF(parquetDF, typeData):
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


def cleanDF(parquetDF):
    parquetDF = parquetDF.na.drop()
    parquetDF = parquetDF.dropDuplicates()

    for cols in ['Crudo', 'Calibraciones', 'Validados']:
        if cols not in parquetDF.columns:
            parquetDF = parquetDF.withColumn(cols, F.lit(None).cast(FloatType()))

    return parquetDF


def returnDF_parquet(parquet, typeData):
    tmpDF      = readDataframe(parquet)  # reemplazar por el ingreso del DF
    typeFilter = typeDF(tmpDF, typeData)
    cleanedDF  = cleanDF(typeFilter)

    return cleanedDF


# ejemplo
# returnDF_parquet(dataframe, "Validados")


spark.stop()