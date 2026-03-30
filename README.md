# Проект YaMDb



### Как запустить проект:

Клонировать репозиторий и перейти в него в командной строке:

```
git clone https://github.com/yandex-praktikum/api_final_yatube
```

```
cd api_final_yatube
```

Cоздать и активировать виртуальное окружение:

```
python3 -m venv env
```

```
source env/bin/activate
```

Установить зависимости из файла requirements.txt:

```
python3 -m pip install --upgrade pip
```

```
pip install -r requirements.txt
```



Запустить проект:

```
python3 test3.py
```


### Конфиг
Регистрация нового пользователя
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
  retry_attempts: 3

parser:
  company_id: 4181
  company_name: null
  date_from: null
  days_back: 7
  db_enabled: True
  output_file: vacancies.csv
  no_csv: false



csv_log:
  log_file: parsing_log.csv
  no_log: false
```
Запрос
```
{
  "email": "user@example.com",
  "username": "^w\\Z"
}
```
Ответ:
```
{
  "email": "string",
  "username": "string"
}
```
Получение JWT токена
```
http://127.0.0.1:8000/api/v1/auth/token/
```
Запрос
```
{
  "username": "^w\\Z",
  "confirmation_code": "string"
}
```
Ответ:
```
{
  "token": "string"
}
```
Получение комментариев
```
http://127.0.0.1:8000/api/v1/titles/Title1/reviews/Review2/comments/
```
Ответ:
```
{
  "count": 0,
  "next": "string",
  "previous": "string",
  "results": [
    {
      "id": 0,
      "text": "string",
      "author": "string",
      "pub_date": "2019-08-24T14:15:22Z"
    }
  ]
}
```
### Стек использованных технологий
### Язык программирования и фреймворк:
  Python: основной язык программирования.
  Django: основной веб-фреймворк.
  Django REST Framework (DRF): расширение Django для создания RESTful веб-сервисов.
### База данных:
  SQLite: проще в настройке, хороша для начального этапа разработки и тестирования.
### Аутентификация и авторизация:
  Django Rest Framework Simple JWT: библиотека для работы с JWT в DRF.
### Документация API:
  ReDoc: генератор статической документации.
### Тестирование:
  Pytest: для написания тестов.
  pytest-django: для расширения Pytest функционалом для Django.

### Информация об авторах
Герасимов Степан
Чугунова Анастасия
Широбоков Павел
