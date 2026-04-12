CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(150) UNIQUE NOT NULL,
    city VARCHAR(100),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INT REFERENCES customers(customer_id),
    product VARCHAR(200) NOT NULL,
    amount DECIMAL(10,2) NOT NULL,
    status VARCHAR(20) DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Seed data
INSERT INTO customers (name, email, city) VALUES
('Ali Hassan', 'ali@example.com', 'Karachi'),
('Sara Ahmed', 'sara@example.com', 'Lahore'),
('Bilal Khan', 'bilal@example.com', 'Islamabad'),
('Fatima Noor', 'fatima@example.com', 'Sukkur'),
('Omar Raza', 'omar@example.com', 'Hyderabad');

INSERT INTO orders (customer_id, product, amount, status) VALUES
(1, 'Laptop', 85000.00, 'completed'),
(2, 'Headphones', 3500.00, 'pending'),
(3, 'Keyboard', 4500.00, 'completed'),
(1, 'Mouse', 1500.00, 'shipped'),
(4, 'Monitor', 32000.00, 'pending');
