#!/usr/bin/env python3
import csv
import os
import re
import sys
from datetime import datetime


DATA_FILES = {
    "movies": "movies.csv",
    "ratings": "ratings.csv",
    "tags": "tags.csv",
    "users": "users.txt",
}


def detect_movies_schema(sample_row):
    return [
        ("id", "INTEGER PRIMARY KEY"),
        ("title", "TEXT NOT NULL"),
        ("year", "INTEGER"),
        ("genres", "TEXT")
    ]


def detect_ratings_schema(sample_row):
    return [
        ("id", "INTEGER PRIMARY KEY"),
        ("user_id", "INTEGER NOT NULL"),
        ("movie_id", "INTEGER NOT NULL"),
        ("rating", "REAL NOT NULL"),
        ("timestamp", "INTEGER NOT NULL")
    ]


def detect_tags_schema(sample_row):
    return [
        ("id", "INTEGER PRIMARY KEY"),
        ("user_id", "INTEGER NOT NULL"),
        ("movie_id", "INTEGER NOT NULL"),
        ("tag", "TEXT NOT NULL"),
        ("timestamp", "INTEGER NOT NULL")
    ]


def detect_users_schema(sample_row):
    return [
        ("id", "INTEGER PRIMARY KEY"),
        ("name", "TEXT NOT NULL"),
        ("email", "TEXT NOT NULL"),
        ("gender", "TEXT"),
        ("register_date", "TEXT NOT NULL"),
        ("occupation", "TEXT")
    ]


def sql_quote(value):
    if value is None:
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"


def try_int(value):
    try:
        return int(value)
    except:
        return None


def try_float(value):
    try:
        return float(value)
    except:
        return None


def read_csv_rows(path, delimiter=','):
    with open(path, 'r', encoding='utf-8') as f:
        first_line = f.readline()
        f.seek(0)
        has_header = False
        if any(h in first_line.lower() for h in ["id", "title", "user_id", "movie_id", "rating", "timestamp", "name", "email"]):
            has_header = True
        reader = csv.reader(f, delimiter=delimiter)
        if has_header:
            next(reader, None)
        for row in reader:
            if row and any(col.strip() for col in row):
                yield row


year_pattern = re.compile(r"\((\d{4})\)\s*$")


def extract_year_and_clean_title(raw_title):
    if raw_title is None:
        return None, None
    title = raw_title.strip()
    year = None
    m = year_pattern.search(title)
    if m:
        try:
            year = int(m.group(1))
        except Exception:
            year = None
        title = year_pattern.sub("", title).rstrip()
        if title.endswith(","):
            title = title[:-1].rstrip()
    return title, year


def generate_create_table_sql(table_name, columns):
    cols_sql = ",\n    ".join([f"{name} {ctype}" for name, ctype in columns])
    return f"DROP TABLE IF EXISTS {table_name};\nCREATE TABLE {table_name} (\n    {cols_sql}\n);\n"


def generate_insert_sql(table_name, columns, rows):
    col_names = [c[0] for c in columns]
    values_sql = []
    for r in rows:
        vals = []
        for name, ctype in columns:
            idx = col_names.index(name)
            v = r[idx] if idx < len(r) else None
            if v is None or v == '':
                vals.append("NULL")
                continue
            if "INTEGER" in ctype:
                iv = try_int(v)
                vals.append("NULL" if iv is None else str(iv))
            elif "REAL" in ctype:
                fv = try_float(v)
                vals.append("NULL" if fv is None else str(fv))
            else:
                vals.append(sql_quote(v))
        values_sql.append("(" + ", ".join(vals) + ")")
    if not values_sql:
        return ""
    return f"INSERT INTO {table_name} (" + ", ".join(col_names) + ") VALUES\n" + ",\n".join(values_sql) + ";\n"


def main():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    paths = {k: os.path.join(base_dir, v) for k, v in DATA_FILES.items()}

    missing = [k for k, p in paths.items() if not os.path.exists(p)]
    if missing:
        print("Отсутствуют входные файлы:", ", ".join(missing))
        sys.exit(1)

    raw_movie_rows = list(read_csv_rows(paths["movies"], delimiter=','))
    movies_columns = detect_movies_schema(raw_movie_rows[0] if raw_movie_rows else [])
    movie_rows = []
    for r in raw_movie_rows:
        movie_id = r[0] if len(r) > 0 else None
        raw_title = r[1] if len(r) > 1 else None
        genres = r[2] if len(r) > 2 else None
        clean_title, year = extract_year_and_clean_title(raw_title)
        movie_rows.append([movie_id, clean_title, year, genres])

    rating_rows = list(read_csv_rows(paths["ratings"], delimiter=','))
    ratings_columns = detect_ratings_schema(rating_rows[0] if rating_rows else [])

    tag_rows = list(read_csv_rows(paths["tags"], delimiter=','))
    tags_columns = detect_tags_schema(tag_rows[0] if tag_rows else [])

    user_rows = list(read_csv_rows(paths["users"], delimiter='|'))
    users_columns = detect_users_schema(user_rows[0] if user_rows else [])

    sql_parts = []
    sql_parts.append("-- SQLite init script for movies_rating.db")
    sql_parts.append("PRAGMA foreign_keys = OFF;")
    sql_parts.append("BEGIN TRANSACTION;")

    sql_parts.append(generate_create_table_sql("movies", movies_columns))
    sql_parts.append(generate_create_table_sql("ratings", ratings_columns))
    sql_parts.append(generate_create_table_sql("tags", tags_columns))
    sql_parts.append(generate_create_table_sql("users", users_columns))

    if movie_rows:
        sql_parts.append(generate_insert_sql("movies", movies_columns, movie_rows))
    if rating_rows:
        sql_parts.append(generate_insert_sql("ratings", ratings_columns, rating_rows))
    if tag_rows:
        sql_parts.append(generate_insert_sql("tags", tags_columns, tag_rows))
    if user_rows:
        sql_parts.append(generate_insert_sql("users", users_columns, user_rows))

    sql_parts.append("COMMIT;")

    out_path = os.path.join(base_dir, "db_init.sql")
    with open(out_path, 'w', encoding='utf-8') as f:
        f.write("\n\n".join([p for p in sql_parts if p]))

    print(f"Скрипт успешно создан: {out_path}")


if __name__ == "__main__":
    main()


