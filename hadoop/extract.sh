START="$1"
END="$2"

cat >config.yml <<EOF
in:
    # MongoDBから指定した日付のレコードを抽出
    type: mongodb
    uri: mongodb://localhost:27017/twitter
    collection: sample
    query: '{ "_timestamp": { \$gte: "${START}", \$lt: "${END}" }}'
    projection: '{ "timestamp_ms": 1, "lang": 1, "text": 1 }'
out:
    # JSONファイルとして出力
    type: file
    path_prefix: /tmp/twitter_sample_${START}/
    file_ext: json.gz
    formatter:
        type: jsonl
    encoders:
    - type: gzip
EOF

# 出力ディレクトリの飾花
rm -rf /tmp/twitter_sample_${START}
mkdir /tmp/twitter_sample_${START}

# データ抽出を実行
embulk run config.yml
