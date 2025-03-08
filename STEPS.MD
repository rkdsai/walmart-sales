updated
```
src\projectFiles\utils\common.py

and 

src\projectFiles\__init__.py
```


TO TEST LOGGER

```from pathlib import Path
from projectFiles.utils.common import read_yaml, create_directories, save_json, load_json
import logging

# Set up test paths
test_yaml_path = Path("test.yaml")
test_json_path = Path("test.json")
test_dirs = [Path("test_dir")]

# Create a sample YAML file
test_yaml_content = {"key": "value"}
test_yaml_path.write_text("key: value")

# Test functions and check logs
try:
    print("Testing read_yaml...")
    config = read_yaml(test_yaml_path)
    print("Output:", config)

    print("\nTesting create_directories...")
    create_directories(test_dirs)

    print("\nTesting save_json...")
    save_json(test_json_path, test_yaml_content)

    print("\nTesting load_json...")
    json_data = load_json(test_json_path)
    print("Output:", json_data)

except Exception as e:
    logging.error(f"Test failed: {e}")

print("\nCheck the logs in 'logs/project.log' for details.")
```

raw file updated from github

## Data Ingestion

updated 
1. src\projectFiles\constants\__init__.py
2. params.yaml
3. schema.yaml

tested in 
1. research\data_ingestion.ipynb
got output in artifacts
1. data.zip
2. features.csv.zip
3. sampleSubmission.csv.zip
4. stores.csv
5. test.csv.zip
6. train.csv.zip

Implemented data ingestion in modular coding

updated
1. config.yaml
2. Update schema.yaml
3. Update params.yaml
4. Update the entity
5. Update the configuration manager in src config
6. Update the components
7. Update the pipeline 
8. Update the main.py

## Data Validation

tested in 
1. research\data_ingestion.ipynb

Implemented data validation in modular coding

updated
1. Update the entity
2. Update the configuration manager in src config
3. Update the components
4. Update the pipeline 
5. Update the main.py

## Data Transformation

updated config/config.yaml 
tested in 
1. research\data_ingestion.ipynb

Implemented data validation in modular coding

updated
1. Update the entity
2. Update the configuration manager in src config
3. Update the components
4. Update the pipeline 
5. Update the main.py