
# Environment Setup


## Create the virtual environment

```
python -m venv final-project
source final-project/bin/activate

pip install pyspark torch transformers huggingface_hub

```


## Set environment variables to use the virtual environment
```
export PYSPARK_PYTHON=~/final-project/final-project/bin/python
export PYSPARK_DRIVER_PYTHON=~/final-project/final-project/bin/python
```

## Start Spark Shell
```
pyspark --deploy-mode client
```

You can use the below to check your environment
```
import sys
print(sys.executable)

```

