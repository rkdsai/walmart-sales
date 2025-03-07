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