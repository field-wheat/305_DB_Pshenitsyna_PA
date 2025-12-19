# Лабораторная работа 2. Подготовка скриптов для создания таблиц и добавления данных

## Требования к окружению

Для корректной работы скрипта db_init.bat необходимо следующее ПО:

### Обязательные компоненты:

1. **Python 3.6 или выше**  
   - Должен быть доступен в PATH как *python3* или *python*  
   - Проверка установки: python --version или python3 --version
1. **SQLite3**
   - Должен быть доступен в PATH как *sqlite3*  
   - Проверка установки: sqlite3 --version

### Дополнительные требования:
- Операционная система: Windows, Linux или macOS
- Исходные файлы movies.csv, ratings.csv, tags.csv, users.txt в папке Task02

## Структура БД:

- *movies*. Поля id (primary key), title, year, genres.
- *ratings*. Поля id (primary key), user_id, movie_id, rating, timestamp.
- *tags*. Поля id (primary key), user_id, movie_id, tag, timestamp.
- *users*. Поля id (primary key), name, email, gender, register_date, occupation.

## Запуск скрипта

1. Убедитесь, что все требования выполнены  
2. Перейдите в директорию Task02
3. Выполните команду: ./db_init.bat