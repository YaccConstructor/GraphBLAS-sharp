import subprocess
import csv
import os
import json
import pathlib
import platform

from dataclasses import dataclass

BENCHMARKS = pathlib.Path(__file__).parent.parent
CONFIGS = BENCHMARKS / "Configs"
BINARIES = BENCHMARKS / "bin" / "Release" / "net7.0"
RESULTS = BENCHMARKS / "BenchmarkDotNet.Artifacts" / "results"

targets = [line.strip() for line in open(CONFIGS / "WorkflowTargets.txt", 'r').readlines()]

#Clearing previous results
subprocess.call(f'rm {RESULTS / "*"}', shell=True)

#Executing benchmarks
for target in targets:
	subprocess.call(f'dotnet {BINARIES / "GraphBLAS-sharp.Benchmarks.dll"} --exporters briefjson --filter *{target}*', shell=True)

#Parsing matrix names in jsons to draw charts(FullName = matrix.mtx)
json_files = [file for file in os.listdir(RESULTS) if file.endswith(".json")]

for file in json_files:
	with open(RESULTS / file, "r+") as file:
		data = json.load(file)
		for benchmark in data["Benchmarks"]:	
			name_field = benchmark["FullName"] 
			matrix_name = name_field.split(":")[-1][:-1].strip()
			print(matrix_name)
			benchmark["FullName"] = matrix_name

		file.seek(0)
		json.dump(data, file)
		file.truncate()