import sys
import os
import shutil

pm10_filename = "PM10.csv"
pm25_filename = "PM25.csv"

smog_data_path = os.path.join("..", "data", "smog")
smog_history_path = os.path.join(smog_data_path, "history")
pm10_path = os.path.join(smog_data_path, "pm10_history")
pm25_path = os.path.join(smog_data_path, "pm25_history")

os.makedirs(pm10_path, exist_ok=True)
os.makedirs(pm25_path, exist_ok=True)

for root, dirs, files in os.walk(smog_history_path):
    for name in dirs:
        dir = os.path.join(root, name)

        pm10_file = os.path.join(dir, pm10_filename)
        shutil.copy(pm10_file, pm10_path)
        os.rename(os.path.join(pm10_path, pm10_filename), os.path.join(pm10_path, f"{name}_{pm10_filename}"))

        pm25_file = os.path.join(dir, pm25_filename)
        shutil.copy(pm25_file, pm25_path)
        os.rename(os.path.join(pm25_path, pm25_filename), os.path.join(pm25_path, f"{name}_{pm25_filename}"))
