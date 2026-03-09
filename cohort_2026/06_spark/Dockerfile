FROM jupyter/pyspark-notebook:latest

USER root

# Expose Jupyter and Spark UI ports
EXPOSE 8888 4040

USER ${NB_UID}

WORKDIR /home/jovyan/work
