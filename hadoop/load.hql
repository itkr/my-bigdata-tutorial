-- JSONファイルを読み込むためのライブラリ
ADD JAR /usr/local/Cellar/hive/2.1.1/libexec/hcatalog/share/hcatalog/hive-hcatalog-core-2.1.1.jar;

-- JSONファイルを読み込むための外部テーブル
CREATE TEMPORARY EXTERNAL TABLE twitter_sample(
  record struct<timestamp_ms:string, lang:string, text:string>
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE LOCATION '/tmp/twitter_sample_${START}/';

-- 出力先のテーブル（パーティション分割、ORC形式）
CREATE TABLE IF NOT EXISTS twitter_sample_words(
  time timestamp, word string
)
PARTITIONED BY(pt string) STORED AS ORC;

-- 日付指定でパーティションを上書き
INSERT OVERWRITE TABLE twitter_sample_words PARTITION (pt='${START}')
SELECT from_unixtime(cast(record.timestamp_ms / 1000 AS bigint)) time, word
FROM twitter_sample
LATERAL VIEW explode(split(record.text, '\\s+')) words AS word
WHERE record.lang = 'en' ORDER BY time;
