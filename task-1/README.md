# Автоматизированное развертывание кластера hdfs

## Установка

1. Установите git и python

```
sudo apt install git -y
```

```
sudo apt install python3 -y
```

2. Склонируйте репозиторий командой

```
git clone https://github.com/EkaterinaNikolaeva/hse-data-patforms.git
```

3. Перейдите в директорию проекта

```
cd hse-data-patforms/task-1/
```

4. Для установки необходимых зависимостей выполните

```
python3 main.py prepare
```

Эта команда установит ansible, sshpass.

5. Для развертывания HDFS кластера:

* Замените `<pass>` в файле inventory.ini на реальный пароль

* Выполните

```
python3 main.py run
```

Эта команда запустит playbook `run_hdfs.yml`

6. Для очистки примененных изменений примените

```
python3 main.py clean
```

Эта команда запустит playbook `cleanup.yml`

## Проверка работоспособности

1. Для удобного доступа к интерфейсу можно выполнить команду:

```
ssh -L 9870:192.168.1.7:9870 team@<ip>
```

тогда в браузере можно будет открыть вкладку по адресу localhost:9870

2. Для проверки работоспобности на хостах системы можно выполнить команду

```
sudo -u hadoop jps
```

ожидаемый вывод вида

```
22697 Jps
22524 DataNode
```
