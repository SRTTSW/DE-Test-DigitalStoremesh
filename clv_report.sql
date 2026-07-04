-- %%
SELECT
    c.customer_id                              AS customer_id,            -- รหัสประจำตัวลูกค้า
    c.full_name                                AS full_name,              -- ชื่อ-นามสกุลเต็มของลูกค้า
    COUNT(o.order_id)                          AS total_orders_placed,    -- นับจำนวนคำสั่งซื้อทั้งหมดที่ลูกค้ารายนั้นสั่ง
    ROUND(COALESCE(SUM(o.usd_amount), 0), 2)   AS lifetime_value_usd,     -- ยอดใช้จ่ายรวมตลอดชีพ (หน่วย USD)
    strftime('%Y-%m', c.signup_date)           AS customer_cohort         -- กลุ่มลูกค้าตามเดือนที่สมัคร
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id  -- ลูกค้าที่ยังไม่เคยสั่งซื้อก็จะแสดงผล (ค่าเป็น 0)
GROUP BY c.customer_id, c.full_name, customer_cohort
ORDER BY lifetime_value_usd DESC