# Define env Python to use
export PYSPARK_PYTHON=/usr/bin/python3

# Define IPython driver
export PYSPARK_DRIVER_PYTHON='jupyter'

# Define Spark conf to use
export PYSPARK_DRIVER_PYTHON_OPTS='notebook --ip=172.17.0.2 --port=8889'

/home/hadoop/spark/bin/pyspark
