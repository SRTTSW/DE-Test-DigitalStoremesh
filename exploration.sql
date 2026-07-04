-- %%
-- ---------------------------------------------------------------------
-- 1. CUSTOMERS: customer_id ที่ซ้ำกัน
--    customer_id ที่มีมากกว่า 1 แถว ถือว่าเป็ยข้อมูลที่ซ้ำกัน จะลบออก เก็บแถวที่มีวันที่สมัครสมาชิกล่าสุดเอาไว้.
-- ---------------------------------------------------------------------
SELECT
    customer_id,
    COUNT(*) AS num_records
FROM vw_raw_customers
GROUP BY customer_id
HAVING COUNT(*) > 1           -- แสดงเฉพาะ customer_id ที่มากกว่า 1 แถว

-- %%
-- แสดงข้อมูลที่ซ้ำกัน
SELECT *
FROM vw_raw_customers
WHERE customer_id IN (
    SELECT customer_id
    FROM vw_raw_customers
    GROUP BY customer_id
    HAVING COUNT(*) > 1
)
ORDER BY customer_id, signup_date

-- %%
-- ---------------------------------------------------------------------
-- 2. CUSTOMERS: Missing / NULL emails
-- ---------------------------------------------------------------------
SELECT *
FROM vw_raw_customers
WHERE email IS NULL OR TRIM(email) = ''

-- %%
-- ---------------------------------------------------------------------
-- 3. CUSTOMERS: ข้อมูล phone numbers ที่ขาดหาย และ formats ที่ไม่ถูกต้อง
--    (mixed formats: "+1 (555) 123-4567", "555-987-6543", raw digits,
--     "Ext 444", "1-800-555-DINO" containing letters, NULLs, etc.)
-- ---------------------------------------------------------------------
SELECT customer_id, full_name, phone
FROM vw_raw_customers
WHERE phone IS NULL
   OR TRIM(phone) = ''
   OR phone GLOB '*[A-Za-z]*'

-- %%
-- phone numbers containing letters
-- ---------------------------------------------------------------------
-- 4. ORDERS: total_amount ค่าเป็น 0 หรือตืดลบ
-- ---------------------------------------------------------------------
SELECT *
FROM vw_raw_orders
WHERE total_amount <= 0

-- %%
-- ---------------------------------------------------------------------
-- 5. ORDERS: Missing / NULL currency
-- ---------------------------------------------------------------------
SELECT *
FROM vw_raw_orders
WHERE currency IS NULL OR TRIM(currency) = ''

-- %%
-- ---------------------------------------------------------------------
-- 6. ORDERS: Missing order_date
-- ---------------------------------------------------------------------
SELECT *
FROM vw_raw_orders
WHERE order_date IS NULL OR TRIM(order_date) = ''

-- %%
-- ---------------------------------------------------------------------
-- 7. ORDERS: customer_id ที่ไม่มีจริง
-- ---------------------------------------------------------------------
SELECT o.*
FROM vw_raw_orders o
LEFT JOIN vw_raw_customers c ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL

-- %%
-- ---------------------------------------------------------------------
-- 8. ORDERS: คำสั่งซื้อที่เป็นสกุลเงินอื่น ไม่ใช่ USD
-- ---------------------------------------------------------------------
SELECT o.order_id, o.order_date, o.currency, o.total_amount
FROM vw_raw_orders o
LEFT JOIN vw_exchange_rates r
       ON o.order_date = r.date AND o.currency = r.currency
WHERE o.currency IS NOT NULL
  AND o.currency <> 'USD'
  AND r.rate_to_usd IS NULL

-- %%
-- ---------------------------------------------------------------------
-- 9. ORDERS: ดู status values
-- ---------------------------------------------------------------------
SELECT status, COUNT(*) AS num_orders
FROM vw_raw_orders
GROUP BY status
ORDER BY num_orders DESC;

-- %%
-- ---------------------------------------------------------------------
-- 10. EXCHANGE RATES: ตรวจสอบว่ามีข้อมูลอัตราแลกเปลี่ยนของแต่ละสกุลเงิน ครอบคลุมตั้งแต่วันไหนถึงวันไหน และมีข้อมูลอยู่กี่วัน
-- ---------------------------------------------------------------------
SELECT
    currency,
    MIN(date) AS earliest_rate_date,
    MAX(date) AS latest_rate_date,
    COUNT(*)  AS num_days
FROM vw_exchange_rates
GROUP BY currency