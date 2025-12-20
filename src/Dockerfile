# 1. Use an official Spark image as the base
# This image already has Java and Spark installed
FROM bitnami/spark:3.5.0

# 2. Switch to root to install extra Python libraries
USER root

# 3. Install the specific libraries your code needs
# These are the Python "side" of the connection
RUN pip install pyyaml psycopg2-binary

# 4. Set the working directory inside the container
WORKDIR /app

# 5. Copy everything from your Mac folder into the container
# This includes load_imdb.py, your config folder, and your .jar file
COPY . /app

# 6. The Command: This is what runs when the container starts
# It's exactly the spark-submit command you tried earlier!
CMD ["spark-submit", "--jars", "postgresql-42.7.3.jar", "load_imdb.py"]