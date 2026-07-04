-- 1. เช็กข้อมูลลูกค้าซ้ำยึดตามชื่อ
SELECT full_name, COUNT(*) as count
FROM vw_raw_customers
GROUP BY full_name
HAVING count > 1;

-- 2. เช็กข้อมูลลูกค้าที่อีเมลหรือเบอร์โทรศัพท์ว่างเปล่า
SELECT * 
FROM vw_raw_customers 
WHERE email IS NULL OR phone IS NULL;

-- 3. เช็กยอดออเดอร์ที่น้อยกว่าหรือเท่ากับ 0 หรือติด Error
SELECT * 
FROM vw_raw_orders 
WHERE total_amount <= 0 OR status = 'SYSTEM_ERROR';