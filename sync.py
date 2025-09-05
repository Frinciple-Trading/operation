#!/usr/bin/env python3
import os
import subprocess

# === 配置 ===
SRC_USER = "trade"
SRC_HOST = "10.72.163.219"
SRC_PORT = "22"
SRC_BASE = "/home/trade/UdpCap"

DST_BASE = "/mnt/prod/moneymaking/md_csv"

# 源 -> 目标 映射
DIR_MAP = {
    "shfe_csv": "shfe",
    "ine_csv": "ine",
    "cme_csv": "cme",
}

def sync_dir(src_dir, dst_dir):
    # 确保目标路径存在
    os.makedirs(dst_dir, exist_ok=True)

    print(f"📂 正在同步 {src_dir} -> {dst_dir}")

    cmd = [
        "rsync", "-avz",
        "-e", f"ssh -p {SRC_PORT}",
        "--progress",
        "--update",
        f"{SRC_USER}@{SRC_HOST}:{src_dir}/",   # 注意 / 表示目录内容
        dst_dir
    ]

    try:
        subprocess.run(cmd, check=True)
        print(f"✅ {src_dir} 同步成功")
    except subprocess.CalledProcessError:
        print(f"❌ {src_dir} 同步失败")

def main():
    for src_name, dst_name in DIR_MAP.items():
        src_path = os.path.join(SRC_BASE, src_name)
        dst_path = os.path.join(DST_BASE, dst_name)
        sync_dir(src_path, dst_path)

if __name__ == "__main__":
    main()
