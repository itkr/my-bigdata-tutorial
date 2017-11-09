-- ビューの代わりにWITH構文を用いる
WITH word_category AS (
  SELECT word,
         if(count > 1000, word,
            concat('COUNT=', cast(count AS varchar))) category
  FROM (
    SELECT word, count(*) count FROM twitter_sample_words
    WHERE time BETWEEN DATE '2000-01-01' AND DATE '2100-01-01'
    GROUP BY 1
  ) t
)

-- 1時間ごとのサマリーを集計
SELECT date_trunc('hour', a.time) time, b.category, count(*) count
FROM twitter_sample_words a
LEFT JOIN word_category b ON a.word = b.word
WHERE a.time BETWEEN DATE '2000-01-01' AND DATE '2100-01-01'
GROUP BY 1, 2;
