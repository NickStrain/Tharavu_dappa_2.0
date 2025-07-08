from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
import polars as pl
import chardet
from pathlib import Path
from typing import List, IO, Dict, Any, Union, Optional, Callable
import yaml
import inspect
from collections import defaultdict
from ingest import DataCleaner,DataReader,FrameCleaner

app = FastAPI()

# CORS middleware if you're running frontend and backend separately
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Or ["http://localhost:3000"] in dev
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.post("/upload-yaml")
async def receive_yaml(request: Request):
    body = await request.body()
    yaml_str = body.decode("utf-8")

    print("Received YAML:\n", yaml_str)
    execute_workflow(yaml_str)
    return None



cleaner = DataCleaner()
reader = DataReader()
renamer = FrameCleaner()

FUNCTION_MAP = {
    "read_csv": reader.read_csv,
    "drop_nans": cleaner.drop_nans,
    "rename": renamer.rename
}


workflow_outputs = {}

def resolve_task_args(args):
    """Resolve argument references to actual values from workflow_outputs."""
    resolved_args = {}
    for key, value in args.items():
        if isinstance(value, str) and value in workflow_outputs:
            resolved_args[key] = workflow_outputs[value]  
        else:
            resolved_args[key] = value  
    return resolved_args

def execute_task(single_node,i, task_number):
  
    # task_id = nodes["id"]
    # function_name = task["function"]
    
    task_id = i 
    function_name = single_node["function"]

    if function_name not in FUNCTION_MAP:
        print(f"[TASK {task_number}] Skipping {task_id}: Function {function_name} not found.")
        return None

    func = FUNCTION_MAP[function_name]
    args = resolve_task_args(single_node["params"])  
    # args = single_node["params"]
    
    try:
        if "df" in args and not isinstance(args["df"], pl.DataFrame):
            raise TypeError(f"[TASK {task_number}] Error: Expected 'df' to be a Polars DataFrame, but got {type(args['df'])}")

        print(f"[TASK {task_number}] Running {task_id} ({function_name}) with args: {args}")
        result = func(**args)
        print(result)
        print(f"[TASK {task_number}] {task_id} completed.")

        
        if "vars" in single_node and isinstance(single_node["vars"], str):
            workflow_outputs[single_node["vars"]] = result  
            print(f"[TASK {task_number}] Output saved as '{single_node['vars']}'")

        return result
    except Exception as e:
        print(f"[TASK {task_number}] Error in {task_id}: {e}")
    return None

def execute_workflow(yamlstring):
    
    # Load YAML
    # with open(yaml_path, "r") as f:
    #     workflow = yaml.safe_load(f)

 
    workflow = yaml.safe_load(yamlstring)
    nodes = workflow['nodes']
    # print(nodes)
    task_counter = 1  # Start numbering tasks
    for i in nodes:
        single_node = nodes[i]
        function_id = single_node["function"]
        result = execute_task(single_node,i, task_counter)
        task_counter += 1

        
        # if result is not None and "vars" in task:
        #     workflow_outputs[task["vars"]] = result
    
    return None   


#  nodes:
#   load_data:
#     function: load_csv
#     params:
#       filepath: /path/to/data.csv
#   preprocess_data:
#     function: normalize_features
#     params:
#       method: min_max
#     dependencies:
#       - load_data
#   train_model:
#     function: random_forest_train
#     params:
#       n_estimators: 100
#       max_depth: 5
#     dependencies:
#       - preprocess_data
#   evaluate_model:
#     function: calculate_accuracy
#     params:
#       metric: f1_score
#     dependencies:
#       - train_model