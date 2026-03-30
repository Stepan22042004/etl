# ETL процесс для парсинга ВТБ вакансий с HH.ru
Есть конфигурация в отдельном файле, логирование в csv и бд(можно отключить в конфиге), выгрузка вакансий в csv(можно отключить в конфиге), генерация уникального etl_id и прокидывание его в log и бд и retry(кол-во попыток можно настроить в конфиге).
Гидра генерит свою папку outputs с каждым запуском и отдельным логом.



### Как запустить проект:

Клонировать репозиторий и перейти в него в командной строке:

```
git clone https://github.com/Stepan22042004/etl.git
```

```
cd etl
```

Cоздать и активировать виртуальное окружение:

```
python -m venv env
```

```
source env/bin/activate
```

Установить зависимости из файла requirements.txt:

```
python -m pip install --upgrade pip
```

```
pip install -r requirements.txt
```



Запустить проект:

```
python test3.py
```
Если хотим по расписанию парсить, пользуемся crontab как на занятии...


### Конфиг
```
# conf/config.yaml
db:
  name: ${oc.env:HH_DB_NAME, etl}
  user: ${oc.env:HH_DB_USER, etl}
  password: ${oc.env:HH_DB_PASSWORD, etl}
  host: ${oc.env:HH_DB_HOST, localhost}
  port: ${oc.env:HH_DB_PORT, 5432}

api:
  hh_url: "https://api.hh.ru/"
  user_agent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
  per_page: 100
  retry_attempts: 3 (кол-во попыток)

parser:
  company_id: 4181
  company_name: null
  date_from: null (с какой даты)
  days_back: 7 - за какое кол-во дней с момента запуска парсинга(null парсит все вакансии с сайта)
  db_enabled: True - выгрузка в бд
  output_file: vacancies.csv
  no_csv: false - выгрузка в csv



csv_log:
  log_file: parsing_log.csv 
  no_log: false
```

