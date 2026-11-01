# F1 (Olist Brazilian E-Commerce) to MercaAndes Schema Mapping

Source: Kaggle `olistbr/brazilian-ecommerce` (9 CSV files).
Frequency: One-time definitive load (not recurrent).
Objective: Map the 8 Olist entities to the MercaAndes data model.

## CSV Files in the Dataset (9 files)


| # | File                                    | Entity               |
| --- | ----------------------------------------- | ---------------------- |
| 1 | `customers.csv`                         | Customers            |
| 2 | `sellers.csv`                           | Sellers              |
| 3 | `products.csv`                          | Products             |
| 4 | `geolocation.csv`                       | Geolocation          |
| 5 | `orders.csv`                            | Orders               |
| 6 | `order_items.csv`                       | Order Items          |
| 7 | `order_payments.csv`                    | Payments             |
| 8 | `order_reviews.csv`                     | Reviews              |
| 9 | `product_category_name_translation.csv` | Category Translation |

## Field Mapping: Olist → MercaAndes


| Olist Entity | File                 | Olist Field                 | MercaAndes Entity | MercaAndes Field | Transformation / Note                                                                      |
| -------------- | ---------------------- | ----------------------------- | ------------------- | ------------------ | -------------------------------------------------------------------------------------------- |
| orders       | `orders.csv`         | order_id                    | orders            | order_id         | PK direct                                                                                  |
| orders       | `orders.csv`         | customer_id                 | customers         | customer_id      | FK                                                                                         |
| orders       | `orders.csv`         | order_status                | orders            | status           | delivered → completed; canceled → cancelled; processing → in_process                    |
| orders       | `orders.csv`         | order_purchase_timestamp    | orders            | order_date       | ISO timestamp                                                                              |
| order_items  | `order_items.csv`    | order_id                    | order_items       | order_id         | FK                                                                                         |
| order_items  | `order_items.csv`    | product_id                  | products          | product_id       | FK                                                                                         |
| order_items  | `order_items.csv`    | seller_id                   | sellers           | seller_id        | FK                                                                                         |
| order_items  | `order_items.csv`    | price                       | order_items       | unit_price       | Direct                                                                                     |
| order_items  | `order_items.csv`    | freight_value               | order_items       | freight_value    | Direct                                                                                     |
| payments     | `order_payments.csv` | order_id                    | payments          | order_id         | FK                                                                                         |
| payments     | `order_payments.csv` | payment_type                | payments          | method           | credit_card → credit_card; boleto → boleto; voucher → voucher; debit_card → debit_card |
| payments     | `order_payments.csv` | payment_installments        | payments          | installments     | Direct                                                                                     |
| payments     | `order_payments.csv` | payment_value               | payments          | amount           | Direct                                                                                     |
| reviews      | `order_reviews.csv`  | order_id                    | reviews           | order_id         | FK                                                                                         |
| reviews      | `order_reviews.csv`  | review_score                | reviews           | rating           | 1-5 direct                                                                                 |
| reviews      | `order_reviews.csv`  | review_comment_title        | reviews           | comment_title    | Direct                                                                                     |
| products     | `products.csv`       | product_id                  | products          | product_id       | PK direct                                                                                  |
| products     | `products.csv`       | product_category_name       | products          | category         | Portuguese category → Spanish via translation file                                        |
| customers    | `customers.csv`      | customer_id                 | customers         | customer_id      | PK direct                                                                                  |
| customers    | `customers.csv`      | customer_city               | customers         | city             | Direct                                                                                     |
| customers    | `customers.csv`      | customer_state              | customers         | state            | Direct                                                                                     |
| geolocation  | `geolocation.csv`    | geolocation_zip_code_prefix | geolocation       | zip_prefix       | Direct                                                                                     |
| geolocation  | `geolocation.csv`    | geolocation_city            | geolocation       | city             | Direct                                                                                     |
| geolocation  | `geolocation.csv`    | geolocation_state           | geolocation       | state            | Direct                                                                                     |

## Seed Requirements (Section 5.2)

1. **Fixed seed:** `random.seed(42)` / `np.random.seed(42)`.
2. **Idempotent:** `INSERT ... ON CONFLICT DO NOTHING` (or verify existence before insert).
3. **Parametrizable volume:** `--mode dev` (small sample) / `--mode full` (all files).
4. **Referential integrity:** Load order: `customers → sellers → products → geolocation → orders → order_items → payments → reviews`.
5. **Quality defects (5.3):** Configurable defect rate (`F1_DEFECT_RATE`) to inject null dates, negative prices, invalid SKUs.
6. **Local and AWS:** Same script with `DB_HOST=localhost` (local) or AWS RDS endpoint.
