CREATE TABLE IF NOT EXISTS products (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL
);

INSERT INTO products (id, name) VALUES
(1, 'T-shirt'),
(2, 'Sneakers'),
(3, 'Mug')
ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS orders (
    order_id UUID PRIMARY KEY,
    product_id INT REFERENCES products(id),
    product_name TEXT,
    amount NUMERIC,
    created_at TIMESTAMP
);
