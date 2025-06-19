DROP TABLE IF EXISTS order_items, orders, customers, products;

CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100),
    city VARCHAR(50)
);

CREATE TABLE products (
    product_id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    price DECIMAL(10,2),
    stock INT
);

CREATE TABLE orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INT REFERENCES customers(customer_id),
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE order_items (
    item_id SERIAL PRIMARY KEY,
    order_id INT REFERENCES orders(order_id),
    product_id INT REFERENCES products(product_id),
    quantity INT
);

INSERT INTO customers (name, email, city) VALUES
('Alice Johnson', 'alice@example.com', 'Mumbai'),
('Bob Smith', 'bob@example.com', 'Delhi'),
('Charlie Brown', 'charlie@example.com', 'Bangalore');

INSERT INTO products (name, price, stock) VALUES
('Laptop', 75000.00, 10),
('Headphones', 2500.00, 50),
('Keyboard', 1500.00, 30),
('Mouse', 800.00, 100);

INSERT INTO orders (customer_id, order_date) VALUES
(1, NOW() - INTERVAL '10 minutes'),
(2, NOW() - INTERVAL '5 minutes'),
(3, NOW());

INSERT INTO order_items (order_id, product_id, quantity) VALUES
(1, 1, 1),
(1, 2, 2),
(2, 3, 1),
(3, 4, 3);