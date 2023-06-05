CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    email character varying(255) NOT NULL UNIQUE,
    name character varying(255) NOT NULL,
    password character varying(255) NOT NULL
);

INSERT INTO users (id, email, name, password)
VALUES
(1, 'johndoe@example.com', 'John Doe', 'johndoe123'),
(2, 'janedoe@example.com', 'Jane Doe', 'janedoe123'),
(3, 'bobsmith@example.com', 'Bob Smith', 'bobsmith123'),
(4, 'sarahjones@example.com', 'Sarah Jones', 'sarahjones123'),
(5, 'mikejohnson@example.com', 'Mike Johnson', 'mikejohnson123'),
(6, 'emilydavis@example.com', 'Emily Davis', 'emilydavis123'),
(7, 'tomwilson@example.com', 'Tom Wilson', 'tomwilson123'),
(8, 'lauramiller@example.com', 'Laura Miller', 'lauramiller123'),
(9, 'stevebrown@example.com', 'Steve Brown', 'stevebrown123'),
(10, 'karenwhite@example.com', 'Karen White', 'karenwhite123');
COMMIT;