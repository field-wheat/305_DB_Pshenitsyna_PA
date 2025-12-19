-- Active: 1764336625806@@127.0.0.1@3306

-- Создать команды для добавления пяти новых пользователей, в том числе себя и четырех ближайших 
-- соседей по списку вашей группы. Дата регистрации должна определяться по системному времени.

INSERT INTO users (name, email, gender, register_date, occupation) 
VALUES ('Пшеницына Полина', 'Wheatfield@gmail.com', 'female', datetime('now', 'localtime'), 'student');

INSERT INTO users (name, email, gender, register_date, occupation) 
VALUES ('Полковников Дмитрий', 'polcovnicovv@gmail.com', 'male', datetime('now', 'localtime'), 'student');

INSERT INTO users (name, email, gender, register_date, occupation) 
VALUES ('Пузаков Дмитрий', 'puzakovvvvv@gmail.com', 'male', datetime('now', 'localtime'), 'student');

INSERT INTO users (name, email, gender, register_date, occupation) 
VALUES ('Пяткин Игорь', 'pyatkin@gmail.com', 'male', datetime('now', 'localtime'), 'student');

INSERT INTO users (name, email, gender, register_date, occupation) 
VALUES ('Рыбаков Евгений', 'rybakovevgeniy@gmail.com', 'male', datetime('now', 'localtime'), 'student');

--Создать команды для добавления трех новых фильмов разных жанров.

INSERT INTO movies (title, year)
VALUES ('Legally Blonde', 2001);

INSERT INTO movie_genres (movie_id, genre_id)
VALUES (
    (SELECT id FROM movies WHERE title = 'Legally Blonde' AND year = 2001),
    (SELECT id FROM genres WHERE name = 'Comedy')
);

INSERT INTO movies (title, year)
VALUES ('The Imitation Game', 2014);

INSERT INTO movie_genres (movie_id, genre_id)
VALUES (
    (SELECT id FROM movies WHERE title = 'The Imitation Game' AND year = 2014),
    (SELECT id FROM genres WHERE name = 'Thriller')
);

INSERT INTO movies (title, year)
VALUES ('knives Out', 2019);

INSERT INTO movie_genres (movie_id, genre_id)
VALUES (
    (SELECT id FROM movies WHERE title = 'Knives Out' AND year = 2019),
    (SELECT id FROM genres WHERE name = 'Mystery')
);

--Создать команды для добавления трех новых отзывов о добавленных фильмах от себя.

INSERT INTO ratings (user_id, movie_id, rating, timestamp)
VALUES (
    (SELECT id FROM users WHERE email = 'vv1nc3ntt@gmail.com'),
    (SELECT id FROM movies WHERE title = 'Legally Blonde' AND year = 2001),
    4.9,
    strftime('%s', 'now')
);

INSERT INTO ratings (user_id, movie_id, rating, timestamp)
VALUES (
    (SELECT id FROM users WHERE email = 'vv1nc3ntt@gmail.com'),
    (SELECT id FROM movies WHERE title = 'The Imitation Game' AND year = 2014),
    5.0,
    strftime('%s', 'now')
);

INSERT INTO ratings (user_id, movie_id, rating, timestamp)
VALUES (
    (SELECT id FROM users WHERE email = 'vv1nc3ntt@gmail.com'),
    (SELECT id FROM movies WHERE title = 'Knives Out' AND year = 2019),
    5.0,
    strftime('%s', 'now')
);
