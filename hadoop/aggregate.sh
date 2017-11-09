#!/bin/bash

START="$1"
END="$2"

cat >query.sql <<EOF
-- ビューの代わりにWITH構文を用いる
WITH word_category AS (
  SELECT word,
         if(count > 1000, word,
            concat('COUNT=', cast(count AS varchar))) category
  FROM (
    SELECT word, count(*) count FROM twitter_sample_words
    WHERE time BETWEEN DATE '${START}' AND DATE '${END}'
    GROUP BY 1
  ) t
)

-- 1時間ごとのサマリーを集計
SELECT date_trunc('hour', a.time) time, b.category, count(*) count
FROM twitter_sample_words a
LEFT JOIN word_category b ON a.word = b.word
WHERE a.time BETWEEN DATE '${START}' AND DATE '${END}'
GROUP BY 1, 2;
EOF

# Prestoでクエリを実行し、結果をCSVファイルとして出力
presto --catalog hive --schema default -f query.sql \
  --output-format CSV_HEADER > word_summary.csv
