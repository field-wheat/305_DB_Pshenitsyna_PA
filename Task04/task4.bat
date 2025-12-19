#!/bin/bash
chcp 65001

sqlite3 movies_rating.db < db_init.sql

echo "1. Найти все пары пользователей, оценивших один и тот же фильм. Устранить дубликаты, проверить отсутствие пар с самим собой. Для каждой пары должны быть указаны имена пользователей и название фильма, который они ценили. В списке оставить первые 100 записей."
echo --------------------------------------------------
sqlite3 movies_rating.db -box -echo "SELECT u1.name AS user1, u2.name AS user2, m.title FROM ratings r1 JOIN ratings r2 ON r1.movie_id = r2.movie_id JOIN users u1 ON r1.user_id = u1.id JOIN users u2 ON r2.user_id = u2.id JOIN movies m ON r1.movie_id = m.id WHERE r1.user_id < r2.user_id GROUP BY u1.name, u2.name, m.title ORDER BY m.title, u1.name, u2.name LIMIT 100"
echo " "

echo "2. Найти 10 самых старых оценок от разных пользователей, вывести названия фильмов, имена пользователей, оценку, дату отзыва в формате ГГГГ-ММ-ДД."
echo --------------------------------------------------
sqlite3 movies_rating.db -box -echo "SELECT DISTINCT m.title, u.name, r.rating, date(r.timestamp, 'unixepoch') AS review_date FROM ratings r JOIN users u ON r.user_id = u.id JOIN movies m ON r.movie_id = m.id ORDER BY r.timestamp ASC LIMIT 10"
echo " "

echo "3. Вывести в одном списке все фильмы с максимальным средним рейтингом и все фильмы с минимальным средним рейтингом. Общий список отсортировать по году выпуска и названию фильма. В зависимости от рейтинга в колонке Рекомендуем для фильмов должно быть написано Да или Нет."
echo --------------------------------------------------
sqlite3 movies_rating.db -box -echo "WITH avg_ratings AS (SELECT m.id, m.title, m.year, AVG(r.rating) AS avg_rating FROM movies m JOIN ratings r ON m.id = r.movie_id GROUP BY m.id, m.title, m.year), min_max AS (SELECT MIN(avg_rating) AS min_rating, MAX(avg_rating) AS max_rating FROM avg_ratings) SELECT ar.year, ar.title, ar.avg_rating, CASE WHEN ar.avg_rating = mm.max_rating THEN 'Да' ELSE 'Нет' END AS Рекомендуем FROM avg_ratings ar CROSS JOIN min_max mm WHERE ar.avg_rating = mm.min_rating OR ar.avg_rating = mm.max_rating ORDER BY ar.year, ar.title"
echo " "

echo "4. Вычислить количество оценок и среднюю оценку, которую дали фильмам пользователи-мужчины в период с 2011 по 2014 год."
echo --------------------------------------------------
sqlite3 movies_rating.db -box -echo "SELECT COUNT(*) AS количество_оценок, ROUND(AVG(r.rating), 2) AS средняя_оценка FROM ratings r JOIN users u ON r.user_id = u.id WHERE u.gender = 'male' AND strftime('%Y', date(r.timestamp, 'unixepoch')) BETWEEN '2011' AND '2014'"
echo " "

echo "5. Составить список фильмов с указанием средней оценки и количества пользователей, которые их оценили. Полученный список отсортировать по году выпуска и названиям фильмов. В списке оставить первые 20 записей."
echo --------------------------------------------------
sqlite3 movies_rating.db -box -echo "SELECT m.year, m.title, ROUND(AVG(r.rating), 2) AS средняя_оценка, COUNT(DISTINCT r.user_id) AS количество_пользователей FROM movies m JOIN ratings r ON m.id = r.movie_id GROUP BY m.year, m.title ORDER BY m.year, m.title LIMIT 20"
echo " "

echo "6. Определить самый распространенный жанр фильма и количество фильмов в этом жанре. Отдельную таблицу для жанров не использовать, жанры нужно извлекать из таблицы movies."
echo --------------------------------------------------
sqlite3 movies_rating.db -box -echo "WITH RECURSIVE split_genres AS (SELECT id, substr(genres, 1, CASE WHEN instr(genres, '|') > 0 THEN instr(genres, '|') - 1 ELSE length(genres) END) AS genre, CASE WHEN instr(genres, '|') > 0 THEN substr(genres, instr(genres, '|') + 1) ELSE '' END AS remaining FROM movies WHERE genres IS NOT NULL AND genres != '' UNION ALL SELECT id, substr(remaining, 1, CASE WHEN instr(remaining, '|') > 0 THEN instr(remaining, '|') - 1 ELSE length(remaining) END) AS genre, CASE WHEN instr(remaining, '|') > 0 THEN substr(remaining, instr(remaining, '|') + 1) ELSE '' END FROM split_genres WHERE remaining != ''), genre_counts AS (SELECT genre, COUNT(*) AS cnt FROM split_genres GROUP BY genre ORDER BY cnt DESC LIMIT 1) SELECT genre AS жанр, cnt AS количество_фильмов FROM genre_counts"
echo " "

echo "7. Вывести список из 10 последних зарегистрированных пользователей в формате Фамилия Имя|Дата регистрации (сначала фамилия, потом имя)."
echo --------------------------------------------------
sqlite3 movies_rating.db -box -echo "SELECT substr(name, instr(name, ' ') + 1) || ' ' || substr(name, 1, instr(name, ' ') - 1) || '|' || register_date AS результат FROM users ORDER BY register_date DESC LIMIT 10"
echo " "

echo "8. С помощью рекурсивного CTE определить, на какие дни недели приходился ваш день рождения в каждом году."
echo --------------------------------------------------
sqlite3 movies_rating.db -box -echo "WITH RECURSIVE birthday_years AS (SELECT 1990 AS year UNION ALL SELECT year + 1 FROM birthday_years WHERE year < 2024) SELECT year AS год, CASE CAST(strftime('%w', year || '-09-22') AS INTEGER) WHEN 0 THEN 'Воскресенье' WHEN 1 THEN 'Понедельник' WHEN 2 THEN 'Вторник' WHEN 3 THEN 'Среда' WHEN 4 THEN 'Четверг' WHEN 5 THEN 'Пятница' WHEN 6 THEN 'Суббота' END AS день_недели FROM birthday_years"
echo " "