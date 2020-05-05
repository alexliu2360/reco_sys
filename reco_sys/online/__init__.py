# coding='utf8'

# 添加sparkstreaming启动对接kafka的配置
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from setting.default import DefaultConfig

import happybase

#  用于读取hbase缓存结果配置
pool = happybase.ConnectionPool(size=10, host='hadoop-master', port=9090)
# 1、创建conf
conf = SparkConf()
conf.setAll(DefaultConfig.SPARK_ONLINE_CONFIG)
# 建立spark session以及spark streaming context
sc = SparkContext(conf=conf)
# 创建Streaming Context
stream_sc = StreamingContext(sc, 60)

# 基于内容召回配置，用于收集用户行为，获取相似文章实时推荐
similar_kafkaParams = {"metadata.broker.list": DefaultConfig.KAFKA_SERVER, "group.id": 'similar'}
SIMILAR_DS = KafkaUtils.createDirectStream(stream_sc, ['click-trace'], similar_kafkaParams)

# 添加sparkstreaming启动对接kafka的配置
# 配置KAFKA相关，用于热门文章KAFKA读取
click_kafkaParams = {"metadata.broker.list": DefaultConfig.KAFKA_SERVER}
HOT_DS = KafkaUtils.createDirectStream(stream_sc, ['click-trace'], click_kafkaParams)

# new-article，新文章的读取  KAFKA配置
NEW_ARTICLE_DS = KafkaUtils.createDirectStream(stream_sc, ['new-article'], click_kafkaParams)

