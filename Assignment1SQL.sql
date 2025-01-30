USE customerdatabase;

-- Part 4 of Assignment 1
-- Part 4.1
SELECT * FROM customers WHERE registration_date LIKE '2024%';

-- Part 4.2
CREATE TABLE prod_ord AS
SELECT  products.product_id AS product_id, products.product_name, products.category, products.price, products.discounted_price, orders.order_id, orders.customer_id, orders.order_date, orders.quantitiy
FROM products
JOIN orders ON products.product_id = orders.product_id;

SELECT SUM((price-discounted_price) * quantitiy) AS total_discount
FROM prod_ord
WHERE (order_date LIKE '2024%');

-- Part 4.3
SELECT product_id, product_name,
       SUM(
           CASE
               WHEN category = 'Electronics' THEN discounted_price * quantitiy
               ELSE price * quantitiy
           END
       ) AS total_revenue
FROM prod_ord
GROUP BY product_id, product_name;

-- Part 4.4
SELECT customers.customer_id, 
	customers.full_name, 
	customers.email,
	COUNT(DISTINCT orders.order_id) AS total_orders,
	SUM(orders.quantitiy * 
        CASE 
            WHEN products.category = 'Electronics' THEN products.discounted_price
            ELSE products.price
        END
    ) AS total_payment
FROM customers
INNER JOIN orders ON customers.customer_id = orders.customer_id
INNER JOIN products ON orders.product_id = products.product_id
GROUP BY customers.customer_id, customers.full_name, customers.email
HAVING total_orders = (
    SELECT MAX(total_orders) FROM (
        SELECT COUNT(DISTINCT orders.order_id) AS total_orders
        FROM orders
        GROUP BY orders.customer_id
    ) as OrderCounts
)
ORDER BY total_orders DESC;


-- Part 4.5
SELECT DISTINCT  customers.full_name

FROM customers
INNER JOIN orders ON customers.customer_id = orders.customer_id
INNER JOIN products ON orders.product_id = products.product_id
WHERE products.category = "Electronics";



