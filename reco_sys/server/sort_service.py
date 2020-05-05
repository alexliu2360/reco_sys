# coding='utf8'
import os
import sys
# 如果当前代码文件运行测试需要加入修改路径，避免出现后导包问题
from deeplearning.ftrl_keras import LrWithFtrl

BASE_DIR = os.path.dirname(os.path.dirname(os.getcwd()))
sys.path.insert(0, os.path.join(BASE_DIR))

PYSPARK_PYTHON = "/miniconda2/envs/reco_sys/bin/python"
os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON
os.environ["PYSPARK_DRIVER_PYTHON"] = PYSPARK_PYTHON

from server import SORT_SPARK
from pyspark.ml.linalg import DenseVector
from pyspark.ml.classification import LogisticRegressionModel
import pandas as pd
import numpy as np
import tensorflow as tf
from datetime import datetime
import logging

logger = logging.getLogger("recommend")

def lr_sort_service(reco_set, temp, hbu):
    """
    排序返回推荐文章
    :param reco_set:召回合并过滤后的结果
    :param temp: 参数
    :param hbu: Hbase工具
    :return:
    """
    # 排序
    # 1、读取用户特征中心特征
    try:
        user_feature = eval(hbu.get_table_row('ctr_feature_user',
                                              '{}'.format(temp.user_id).encode(),
                                              'channel:{}'.format(temp.channel_id).encode()))
        logger.info("{} INFO get user user_id:{} channel:{} profile data".format(
            datetime.now().strftime('%Y-%m-%d %H:%M:%S'), temp.user_id, temp.channel_id))
    except Exception as e:
        user_feature = []

    if user_feature:
        # 2、读取文章特征中心特征
        result = []

        for article_id in reco_set:
            try:
                article_feature = eval(hbu.get_table_row('ctr_feature_article',
                                                         '{}'.format(article_id).encode(),
                                                         'article:{}'.format(article_id).encode()))
            except Exception as e:

                article_feature = [0.0] * 111
            f = []
            # 第一个channel_id
            f.extend([article_feature[0]])
            # 第二个article_vector
            f.extend(article_feature[11:])
            # 第三个用户权重特征
            f.extend(user_feature)
            # 第四个文章权重特征
            f.extend(article_feature[1:11])
            vector = DenseVector(f)
            result.append([temp.user_id, article_id, vector])

        # 4、预测并进行排序是筛选
        df = pd.DataFrame(result, columns=["user_id", "article_id", "features"])
        test = SORT_SPARK.createDataFrame(df)

        # 加载逻辑回归模型
        model = LogisticRegressionModel.load("hdfs://hadoop-master:9000/headlines/models/LR.obj")
        predict = model.transform(test)

        def vector_to_double(row):
            return float(row.article_id), float(row.probability[1])

        res = predict.select(['article_id', 'probability']).rdd.map(vector_to_double).toDF(
            ['article_id', 'probability']).sort('probability', ascending=False)
        article_list = [i.article_id for i in res.collect()]
        logger.info("{} INFO sorting user_id:{} recommend article".format(datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                                                          temp.user_id))
        # 排序后，只将排名在前100个文章ID返回给用户推荐
        if len(article_list) > 100:
            article_list = article_list[:100]
        reco_set = list(map(int, article_list))

    return reco_set


def lrftrl_sort_service(reco_set, temp, hbu):
    """
    排序返回推荐文章
    :param reco_set:召回合并过滤后的结果
    :param temp: 参数
    :param hbu: Hbase工具
    :return:
    """
    print(344565)
    # 排序
    # 1、读取用户特征中心特征
    try:
        user_feature = eval(hbu.get_table_row('ctr_feature_user',
                                              '{}'.format(temp.user_id).encode(),
                                              'channel:{}'.format(temp.channel_id).encode()))
        logger.info("{} INFO get user user_id:{} channel:{} profile data".format(
            datetime.now().strftime('%Y-%m-%d %H:%M:%S'), temp.user_id, temp.channel_id))
    except Exception as e:
        user_feature = []
        logger.info("{} WARN get user user_id:{} channel:{} profile data failed".format(
            datetime.now().strftime('%Y-%m-%d %H:%M:%S'), temp.user_id, temp.channel_id))
    reco_set = [13295, 44020, 14335, 4402, 2, 14839, 44024, 18427, 43997, 17375]
    if user_feature and reco_set:
        # 2、读取文章特征中心特征
        result = []
        for article_id in reco_set:
            try:
                article_feature = eval(hbu.get_table_row('ctr_feature_article',
                                                         '{}'.format(article_id).encode(),
                                                         'article:{}'.format(article_id).encode()))
            except Exception as e:
                article_feature = []

            if not article_feature:
                article_feature = [0.0] * 111
            f = []
            f.extend(user_feature)
            f.extend(article_feature)

            result.append(f)

        # 4、预测并进行排序是筛选
        arr = np.array(result)

        # 加载逻辑回归模型
        lwf = LrWithFtrl()
        print(tf.convert_to_tensor(np.reshape(arr, [len(reco_set), 121])))
        predictions = lwf.predict(tf.constant(arr))

        df = pd.DataFrame(np.concatenate((np.array(reco_set).reshape(len(reco_set), 1), predictions),
                                          axis=1),
                          columns=['article_id', 'prob'])

        df_sort = df.sort_values(by=['prob'], ascending=True)

        # 排序后，只将排名在前100个文章ID返回给用户推荐
        if len(df_sort) > 100:
            reco_set = list(df_sort.iloc[:100, 0])
        reco_set = list(df_sort.iloc[:, 0])

    return reco_set