sudo apt install unzip
curl -L -o ~/downloads/input.zip https://www.kaggle.com/api/v1/datasets/download/wafaaelhusseini/capital-weather-data-1995-2024
cd ~/downloads/
unzip ./input.zip
scp ./history.parquet team-1-nn:~/history.parquet
ssh team-1-nn
sudo cp ~/history.parquet /home/hadoop/
sudo apt install python3-venv
sudo -i -u hadoop
hdfs dfs -mkdir /input/
hdfs dfs -put ./history.parquet /input/
python3 -m venv ~/.env/
source ~/.env/bin/activate
pip install -U pip
pip install pyspark==3.5.6 onetl
beeline -u jdbc:hive2://team-1-nn:5433
CREATE DATABASE test;
!q
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export JAVA_HOME="" // потому что этого пути не сущетсвует, он мешает работе spark'а
python main.py

