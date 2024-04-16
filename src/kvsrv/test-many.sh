#!/bin/bash

# 文件名
output_file="test_results.txt"

# 清空输出文件
echo "" > "$output_file"

# 运行100次测试
for ((i=1; i<=100; i++)); do
    echo "Running test $i..."
    # 执行测试并将结果追加到文件
    go test >> "$output_file"
done

echo "All tests completed. Results are saved in $output_file"
