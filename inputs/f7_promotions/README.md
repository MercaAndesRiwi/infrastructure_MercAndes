# F7 Promotions Input Folder

This folder contains the weekly promotion plan file (F7) provided by the commercial team.

## Expected file

- Name: `F7_promotions.csv` or `F7_promotions.xlsx`
- Location: `data-platform/inputs/f7_promotions/`
- Frequency: Weekly (new file replaces the previous one)

## Expected columns (case-insensitive)


| Column       | Description          | Example        |
| -------------- | ---------------------- | ---------------- |
| sku          | Product SKU          | SKU-001        |
| seller_id    | Seller identifier    | SELLER-101     |
| store_id     | Store identifier     | STORE-001      |
| promotion_id | Unique promotion ID  | PROMO-2026-001 |
| start_date   | Promotion start date | 2026-11-02     |
| end_date     | Promotion end date   | 2026-11-08     |
| discount_pct | Discount percentage  | 15.0           |
| promo_price  | Promotional price    | 85.00          |
| channel      | Sales channel        | online         |

## Sample file

A sample file is provided: `F7_promotions_sample.csv`

Replace it with the real weekly file before running the `f7_promotions_weekly` DAG.
