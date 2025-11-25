## GreenPlum

1. Подключение к машине с GreenPlum

```bash
ssh user@91.185.85.179
```

2. Скачивание и распаковка данных

```
mkdir ~/data/
cd ~/data/
curl -L -o ./data.zip https://www.kaggle.com/api/v1/datasets/download/saurabh00007/iriscsv
unzip ./data.zip
```

3. Запуск gpfdist в отдельном терминале

```bash
gpfdist -d /home/user/data -p 8081
```

4. Подключение к базе

```
psql -d idp
```

5. Создание EXTERNAL таблицы

```sql
CREATE EXTERNAL TABLE ext_iris_team_1 (
    Id               INTEGER,
    SepalLengthCm    FLOAT,
    SepalWidthCm     FLOAT,
    PetalLengthCm    FLOAT,
    PetalWidthCm     FLOAT,
    Species          TEXT
)
LOCATION ('gpfdist://localhost:8081/Iris.csv')
FORMAT 'csv' (delimiter ',' header);
```

6. Создание обычной таблицы

```sql
CREATE TABLE iris_team_1 (
    Id               INTEGER,
    SepalLengthCm    FLOAT,
    SepalWidthCm     FLOAT,
    PetalLengthCm    FLOAT,
    PetalWidthCm     FLOAT,
    Species          TEXT
)
WITH (
    APPENDONLY=true,
    ORIENTATION=column,
    COMPRESSTYPE=zlib
)
DISTRIBUTED BY (Id);
```

7. Загрузка данных

```
INSERT INTO iris_team_1
SELECT * FROM ext_iris_team_1;
```

В итоге будет создана таблица **iris_team_1**.