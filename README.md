# 
<h1 align=center>Big Data : Introduction to Spark</h1>

<p align=center>
  <img src="https://upload.wikimedia.org/wikipedia/commons/thumb/f/f3/Apache_Spark_logo.svg/1200px-Apache_Spark_logo.svg.png" width="250" height="120"/>
  
  <img src="https://upload.wikimedia.org/wikipedia/commons/thumb/f/f8/Python_logo_and_wordmark.svg/1200px-Python_logo_and_wordmark.svg.png"  height="110"/>
</p>


## Requirements
```sh
- python
- PySpark 
- Docker
```

## Using Spark on your machine :
Assuming that you have installed spark on your machine, you can run it by simply executing in a terminal:   

  ```sh
$ python3 ./navigation.py
```


## Using Spark installed with Docker :
**{absolute_path_to_folder}** should be replaced by the actual path to the directory where your Python scripts are stored. 

When you launch this command, logs are written into the terminal. The last displayed line is an url that you should copy into a web browser.    
This url allows you to connect to a Jupyter Notebook that gives access to Spark.

```sh
docker run -v {absolute_path_to_folder}:/home/jovyan/work -it \
       --rm -p 8888:8888 -p 4040:4040 jupyter/pyspark-notebook
```
