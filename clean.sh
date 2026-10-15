#!/bin/bash
# clean_pycache.sh - 清理 __pycache__、logs、snapshots、videos 等缓存与临时目录

echo "========================================="
echo "  清理缓存和临时目录"
echo "========================================="

# 定义要清理的目录列表
DIRS_TO_CLEAN=("__pycache__" "logs" "snapshots" "videos" "dist" "release" "build")

# 定义要清理的文件列表（厂商 SDK 已移除，不再产生相关日志文件，可按需补充）
FILES_TO_CLEAN=()

# 统计总删除数量
total_deleted=0
total_files_deleted=0

# 遍历每个目录类型
for target_dir in "${DIRS_TO_CLEAN[@]}"; do
    echo ""
    echo "正在查找所有 '$target_dir' 目录..."
    
    # 查找所有匹配的目录
    mapfile -t dirs < <(find . -type d -name "$target_dir" 2>/dev/null)
    count=${#dirs[@]}
    
    if [ $count -eq 0 ]; then
        echo "  未找到任何 '$target_dir' 目录"
        continue
    fi
    
    # 删除每个目录
    deleted=0
    for dir in "${dirs[@]}"; do
        echo "    正在删除: $dir"
        rm -rf "$dir" 2>/dev/null
        if [ $? -eq 0 ]; then
            ((deleted++))
        else
            echo "      警告: 删除失败（可能权限不足）"
        fi
    done
    
    echo "  已清理 $deleted 个 '$target_dir' 目录"
    total_deleted=$((total_deleted + deleted))
done

# 清理文件
echo ""
echo "正在查找需要清理的临时文件..."
for target_file in "${FILES_TO_CLEAN[@]}"; do
    # 查找所有匹配的文件
    mapfile -t files < <(find . -type f -name "$target_file" 2>/dev/null)
    count=${#files[@]}
    
    if [ $count -eq 0 ]; then
        echo "  未找到任何 '$target_file' 文件"
        continue
    fi
    
    # 删除每个文件
    deleted=0
    for file in "${files[@]}"; do
        echo "    正在删除: $file"
        rm -f "$file" 2>/dev/null
        if [ $? -eq 0 ]; then
            ((deleted++))
        else
            echo "      警告: 删除失败（可能权限不足）"
        fi
    done
    
    echo "  已清理 $deleted 个 '$target_file' 文件"
    total_files_deleted=$((total_files_deleted + deleted))
done

echo ""
echo "========================================="
echo "✓ 完成！共清理 $total_deleted 个目录和 $total_files_deleted 个文件"
echo "========================================="