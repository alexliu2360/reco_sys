import os
import sys
# 如果当前代码文件运行测试需要加入修改路径，避免出现后导包问题
BASE_DIR = os.path.dirname(os.path.dirname(os.getcwd()))
sys.path.insert(0, os.path.join(BASE_DIR))

# PYTHONUNBUFFERED=1
JAVA_HOME="/root/bigdata/jdk"
SPARK_HOME="/root/bigdata/spark"
HADOOP_HOME="/root/bigdata/hadoop"
PYSPARK_DRIVER_PYTHON="/root/anaconda3/envs/reco_sys/bin/python"
PYSPARK_PYTHON = "/miniconda2/envs/reco_sys/bin/python"
# 当存在多个版本时，不指定很可能会导致出错
os.environ["JAVA_HOME"] = JAVA_HOME
os.environ["SPARK_HOME"] = SPARK_HOME
os.environ["HADOOP_HOME"] = HADOOP_HOME
os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON
os.environ["PYSPARK_DRIVER_PYTHON"] = PYSPARK_PYTHON


from offline import SparkSessionBase
import pyhdfs
import time


class UpdateUserProfile(SparkSessionBase):
    """离线相关处理程序
    """
    SPARK_APP_NAME = "updateUser"
    ENABLE_HIVE_SUPPORT = True

    SPARK_EXECUTOR_MEMORY = "7g"

    def __init__(self):

        self.spark = self._create_spark_session()

uup = UpdateUserProfile()