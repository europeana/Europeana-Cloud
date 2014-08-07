USE %s;
CREATE TABLE IF NOT EXISTS users(username varchar, password varchar, PRIMARY KEY (username));
INSERT INTO users (username, password) VALUES('Robin_Van_Persie', 'Feyenoord');
INSERT INTO users (username, password) VALUES('Cristiano', 'Ronaldo');