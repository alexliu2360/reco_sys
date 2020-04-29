import json
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
        self._user_article_basic_column = [
            "user_id", "action_time", "article_id", "channel_id","shared", "clicked","collected","exposure","read_time"
        ]

    def update_user_action_basic(self):
        '''
        更新用户行为日志到用户行为基础标签表
        :return:
        '''
        self.spark.sql("use profile")

        # 如果hadoop没有今天该日期文件，则没有日志数据，结束
        time_str = time.strftime("%Y-%m-%d", time.localtime())
        _localions = '/user/hive/warehouse/profile.db/user_action/' + time_str
        fs = pyhdfs.HdfsClient(hosts='hadoop-master:50070')
        if fs.exists(_localions):
            # 如果有该文件直接关联，捕获关联重复异常
            try:
                uup.spark.sql("alter table user_action add partition (dt='%s') location '%s'" % (time_str, _localions))
            except Exception as e:
                pass

            sqlDF = uup.spark.sql(
                "select actionTime, readTime, channelId, param.articleId, param.algorithmCombine, param.action, param.userId from user_action where dt={}".format(
                    time_str))

            if sqlDF.collect():
                def _compute(row):
                    # 进行判断行为类型
                    _list = []
                    if row.action == "exposure":
                        for article_id in eval(row.articleId):
                            _list.append(
                                [row.userId, row.actionTime, article_id, row.channelId, False, False, False, True,
                                 row.readTime])
                        return _list
                    else:
                        class Temp(object):
                            shared = False
                            clicked = False
                            collected = False
                            read_time = ""

                        _tp = Temp()
                        if row.action == "share":
                            _tp.shared = True
                        elif row.action == "click":
                            _tp.clicked = True
                        elif row.action == "collect":
                            _tp.collected = True
                        elif row.action == "read":
                            _tp.clicked = True
                        else:
                            pass
                        _list.append(
                            [row.userId, row.actionTime, int(row.articleId), row.channelId, _tp.shared, _tp.clicked,
                             _tp.collected,
                             True,
                             row.readTime])
                        return _list

                # 进行处理
                # 查询内容，将原始日志表数据进行处理
                _res = sqlDF.rdd.flatMap(_compute)
                data = _res.toDF(
                    ["user_id", "action_time", "article_id", "channel_id", "shared", "clicked", "collected", "exposure",
                     "read_time"])
                # 合并历史数据，插入表中
                old = uup.spark.sql("select * from user_article_basic")
                # 由于合并的结果中不是对于user_id和article_id唯一的，一个用户会对文章多种操作
                old.unionAll(data).resgiterTempTable("temptable")
                self.spark.sql(
                    "insert overwrite table user_article_basic select user_id, max(action_time) as action_time, "
                    "article_id, max(channel_id) as channel_id, max(shared) as shared, max(clicked) as clicked, "
                    "max(collected) as collected, max(exposure) as exposure, max(read_time) as read_time from temptable "
                    "group by user_id, article_id"
                )
                return True
            return False

        else:
            return False

    def update_user_label(self):
        '''
        查询用户与文章基础行为信息表结果，合并每个频道的主题词中间表，与中间表关联
        :return:
        '''
        # 获取基本用户行为信息，然后进行文章画像的主题词合并
        uup.spark.sql("use profile")
        # 取出日志中的channel_id
        user_article_ = uup.spark.sql("select * from user_article_basic").drop('channel_id')
        uup.spark.sql('use article')
        article_label = uup.spark.sql("select article_id, channel_id, topics from article_profile")
        # 合并使用文章中正确的channel_id
        click_article_res = user_article_.join(article_label, how='left', on=['article_id'])
        # 将字段的列表爆炸
        import pyspark.sql.functions as F
        click_article_res = click_article_res.withColumn('topic', F.explode('topics')).drop('topics')

        # 计算每个用户对每篇文章的标签的权重
        def compute_weights(rowpartition):
            """处理每个用户对文章的点击数据
            """
            weightsOfaction = {
                "read_min": 1,
                "read_middle": 2,
                "collect": 2,
                "share": 3,
                "click": 5
            }

            import happybase
            from datetime import datetime
            import numpy as np
            #  用于读取hbase缓存结果配置
            pool = happybase.ConnectionPool(size=10, host='192.168.19.137', port=9090)

            # 读取文章的标签数据
            # 计算权重值
            # 时间间隔
            for row in rowpartition:

                t = datetime.now() - datetime.strptime(row.action_time, '%Y-%m-%d %H:%M:%S')
                # 时间衰减系数
                time_exp = 1 / (np.log(t.days + 1) + 1)

                if row.read_time == '':
                    r_t = 0
                else:
                    r_t = int(row.read_time)
                # 浏览时间分数
                is_read = weightsOfaction['read_middle'] if r_t > 1000 else weightsOfaction['read_min']

                # 每个词的权重分数
                weigths = time_exp * (
                        row.shared * weightsOfaction['share'] + row.collected * weightsOfaction['collect'] + row.
                        clicked * weightsOfaction['click'] + is_read)

                with pool.connection() as conn:
                   table = conn.table('user_profile')
                   table.put('user:{}'.format(row.user_id).encode(),
                             {'partial:{}:{}'.format(row.channel_id, row.topic).encode(): json.dumps(
                                 weigths).encode()})
                   conn.close()

        click_article_res.foreachPartition(compute_weights)

    def update_user_info(self):
        '''
        更新用户的基础信息画像
        :return:
        '''
        self.spark.sql('use toutiao')

        user_basic = self.spark.sql('select user_id, gender birthday from user_profile')

        def _update_user_basic(partition):
            '''
            更新用户基本信息
            :param partition:
            :return:
            '''
            from datetime import datetime
            import json
            import happybase

            pool = happybase.ConnectionPool(size=10, host='192.168.19.137', port=9090)
            for row in partition:
                from datetime import date
                age = 0
                if row.birthday !='null':
                    born = datetime.strftime(row.birthday, "%Y-%m-%d")
                    today = datetime.today()
                    age = today.year - born.year -((today.month, today.day)<(born.month, born.day))

                with pool.connection() as conn:
                    table = conn.table("use profile")
                    table.put("user:{}".format(row.user_id).encode(),
                              {"basic:gender".encode():json.dumps(row.gender).encode()})
                    table.put("user:{}".format(row.user_id).encode(),
                              {"basic:birthday".encode():json.dumps(row.age).encode()})
                    conn.close()

            user_basic.foreachPartition(_update_user_basic)


if __name__ == "__main__":
    uup = UpdateUserProfile()
    uup.update_user_action_basic()
    uup.update_user_label()
    uup.update_user_info()