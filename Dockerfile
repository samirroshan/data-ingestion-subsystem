FROM apache/spark:3.5.0
USER root
RUN pip install --no-cache-dir pyyaml psycopg2-binary pandas
WORKDIR /app
COPY . /app
CMD ["/opt/spark/bin/spark-submit", "--jars", "postgresql-42.7.3.jar", "src/load_imdb.py"]