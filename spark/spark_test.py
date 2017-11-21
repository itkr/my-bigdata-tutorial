# coding: utf-8
spark
df = (spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri","mongodb://localhost/twitter.sample").load())
df
spark()
str(spark)
type(spark)
df.createOrReplaceTempView('tweets')
query = "SELECT lang, count(*) cont FROM tweets WHERE delete IS  NULL GROUP BY 1 ORDER BY 2 DESC"
spark.ql(query).show(3)
spark.sql(query).show(3)
query = """
SELECT from_unixtime(timestamp_ms / 1000 time, text
FROM tweets
WHERE lang = 'en'
"""
from pyspark.sql import Row
def text_split(row):
    for word in row.text.split():
        yield Row(time=row.time,word=word)

en_tweets.rdd.take(1)
en_tweets=spark.sql(query)
query
query = "SELECT from_unixtime(timestamp_ms / 1000 time, text FROM tweets WHERE lang = 'en'"
query
en_tweets=spark.sql(query)
query = "SELECT from_unixtime(timestamp_ms / 1000) time, text FROM tweets WHERE lang = 'en'"
en_tweets=spark.sql(query)
en_tweets
en_tweets.rdd.take(1)
en_tweets.rdd.flatMap(text_split).take(2)
text_split
en_tweets.rdd.flatMap(text_split).toDF().show(2)
en_tweets.rdd.flatMap(text_split).toDF().show(2)
get_ipython().magic(u'save ')
get_ipython().magic(u'save ()')
help(save)
words = en_tweets.rdd.flatMap(text_split).toDF()
words.cresteOrReplaceTempView('words')
words.createOrReplaceTempView('words')
query = "SELECT word, count(*) count FROM words GROUP BY 1 ORDER BY 2 DESC"
spark.sql(query).show(3)
words
words.count()
words.write.saveAsTable('twitter_sample_words')
get_ipython().system(u'ls -R spark-warehouse')
twitter_sample_wards
spark.table('twitter_sample_words').count()
query = """
SELECT substr(time, 1, 13) time,
word,
count(*) count
FROM twitter_sample_words
GROUP BY 1,2
"""
spark.sql(query).count()
query = """
SELECT
  t.count,
  count(*) words
FROM (
  SELECT
    word,
    count(*) count
  FROM twitter_sample_words
  GROUP BY 1
  ) t
GROUP BY 1 ORDER BY 1
"""
spark.sql(query).show(3)
query = "SELECT word, count, IF(count > 1000, word, concat('COUNT=', count)) category FROM ( SELECT word, count(*) count FROM twitter_sample_words GROUP BY 1) t"
spark.sql(query).show(5)
spark.sql(query).createOrReplaceTempView('word_category')
query = """
SELECT subsr(a.time, 1, 13) time,
b.category,
count(*) count
FROM twitter_sample_words a
LEFT JOIN word_category b ON a.word = b.word
GROUP BY 1,2
"""
spqrk.sql(query).count()
spark.sql(query).count()
query
query = """
SELECT substr(a.time, 1, 13) time,
b.category,
count(*) count
FROM twitter_sample_words a
LEFT JOIN word_category b ON a.word = b.word
GROUP BY 1,2
"""
spark.sql(query).count()
(spark.sql(query).coalesce(1).write.format('com.databricks.spark.csv').option('header', 'true').save('csv_output'))
get_ipython().system(u'ls csv_output/')
result = spqrk.sql(query).toPandas()
result = spqhark.sql(query).toPandas()
result = spark.sql(query).toPandas()
reesult
result
type(result)
result.head(2)
import pandas as pd
result['time'] = pd.to_datetime(result['time'])
result.head(2)
result.to_csv('word_summary.csv', index=False, encoding='utf-8')
