# ShopData Inc. — Data Engineer Technical Assessment

ETL pipeline ทำหน้าที่ดึงข้อมูลดิบเกี่ยวกับการจัดการออเดอร์มาจากฐานข้อมูล shopdata.db จากนั้นนำมาล้างข้อมูลให้สะอาด แล้วบันทึกลงในฐานข้อมูล SQLite ตัวใหม่ที่พร้อมใช้งานสำหรับการทำวิเคราะห์ (analytics.db) 
เพื่อให้ทีม BI นำไปทำรายงานหา(Customer Lifetime Value: CLV)

## Project structure

```
.
├── shopdata.db          # ฐานข้อมูลต้นทาง
├── exploration.sql      # Part 1: data quality exploration queries
├── transforms.py        # unit-testable cleaning/transform logic
├── pipeline.py           # Part 2: Prefect ETL flow (extract -> transform -> load)
├── clv_report.sql        # Part 4: Customer Lifetime Value query
├── tests/
│   └── test_pipeline.py  # Part 3: unit tests (pytest) ไม่ต้องเชื่อมฐานข้อมูลจริง
├── requirements.txt
└── README.md
```

## Setup

```bash
python3 -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

## Running the pipeline

```bash
python pipeline.py
```

จะไปอ่านไฟล์ shopdata.db เพื่อทำการล้างข้อมูล แล้วสร้างไฟล์ analytics.db ใหม่ข้างในจะมี 2 ตารางหลักคือ: dim_customers (ข้อมูลลูกค้า) และ fct_orders (ข้อมูลออเดอร์) หากระบบบันทึกลง SQLite ล้มเหลวไม่ว่าด้วยเหตุผลใดก็ตาม ระบบจะสลับไปเขียนไฟล์ตระกูล CSV ชื่อ clean_customers.csv และ clean_orders.csv เก็บไว้ในเครื่องให้แทนโดยอัตโนมัติ


```bash
prefect server start 
python pipeline.py
```

## Running the tests

```bash
pytest tests/ -v
```

โค้ดแปลงข้อมูลทั้งหมดใน transforms.py รับและส่งค่ากลับเป็น pandas DataFrame ทำให้สามารถทดสอบระบบโดยไม่ต้องเชื่อมต่อกับฐานข้อมูลจริง
ส่วนไฟล์ pipeline.py เป็นแค่ตัวเอาฟังก์ชันมาครอบด้วย Prefect (@task/@flow) เพื่อควบคุมการทำงาน, ทำบันทึก Log และสั่งรันใหม่หากเกิด error

## Running the SQL

```bash
sqlite3 shopdata.db < exploration.sql
sqlite3 analytics.db < clv_report.sql     # after running pipeline.py
```

---

## Part 1: Data Exploration Findings

การ run ไฟล์ exploration.sql เพื่อส่อง raw data shopdata.db พบปัญหาคุณภาพข้อมูล ดังนี้:

ข้อมูลลูกค้าซ้ำซ้อน: customer_id หมายเลข 1 (Alice Smith) และหมายเลข 2 (Bob Jones) มีชื่อซ้ำกันคนละ 2 แถว โดยมีอีเมล์/เบอร์โทร และวันที่สมัคร (signup_date) แตกต่างกัน ซึ่งดูเหมือนแถวที่มาทีหลังจะเป็นข้อมูลที่อัปเดตล่าสุด (เช่น อีเมลของ Alice เปลี่ยนไป และเบอร์โทรถูกจัดรูปแบบใหม่) จึงเลือกเก็บแถวที่มีวันที่สมัครล่าสุดไว้

Missing emails: Customers 2 (แถวก่อนอัปเดต) และCustomers 8 (Hannah Abbott) ไม่มีข้อมูลอีเมล (ค่าเป็น NULL)

Inconsistent / missing phone formats: ในคอลัมน์เดียวกันมีเบอร์โทรบันทึกมาถึง 5 รูปแบบ เช่น มีเครื่องหมายบวกและวงเล็บ "+1 (555) 123-4567", มีขีด "555-987-6543", ตัวเลขล้วน, free text "Ext 444", และแบบปนตัวอักษร "1-800-555-DINO" นอกจากนี้ยังมีลูกค้า 2 คนที่ไม่มีเบอร์โทรเลย

Orders ติดลบหรือเป็นศูนย์: เจอออเดอร์ 3 รายการที่มียอดเงิน <= 0 (มี -50.00, -100.00, และ 0.00) มองว่าเป็นข้อผิดพลาดของระบบ (System Errors) ต้องตัดออกไป

Orphaned Foreign Keys: ออเดอร์หมายเลข 106 และ 118 ระบุว่าซื้้อโดยลูกค้ารหัส customer_id = 99 แต่พอย้อนไปดูตารางลูกค้ากลับไม่มีรหัสนี้อยู่จริง (เก็บออเดอร์นี้ไว้ใน fct_orders
แต่โน้ตไว้ว่าเป็นปัญหาที่ต้องแจ้งให้เจ้าของระบบต้นทางทราบ)

Missing currency and order_date: มี 2 ออเดอร์ที่คอลัมน์สกุลเงินเป็นค่าว่าง (ให้นับว่าเป็น USD ตามกฎ) และมี 1 ออเดอร์ที่ไม่มีวันที่สั่งซื้อ (ทำให้เอาไป match หาเรทราคาแลกเปลี่ยนไม่ได้ เลยต้องให้นับว่าเป็น USD เช่นกัน)

ตารางเรทแลกเปลี่ยนเงินไม่ครอบคลุม: ตารางเรทราคา vw_exchange_rates มีข้อมูลแค่ช่วงวันที่ 2023-05-01 ถึง 2023-05-05 แต่ออเดอร์จริงลากยาวไปถึงวันที่ 2023-05-14 ดังนั้น ออเดอร์ที่เป็นสกุลเงินต่างประเทศ (เช่น GBP/EUR) ที่เกิดขึ้นหลังวันที่ 5 เป็นต้นไป จะหาเรทราคาไม่เจอ ระบบจะปล่อยผ่านและถือว่ายอดเงินนั้นเป็น USD ไปเลยตาม business rule

## Part 2: Cleaning Rule Decisions (design notes)

การลบลูกค้าซ้ำ: จับกลุ่มด้วย customer_id แล้วเลือกเก็บแถวที่มีค่าวันที่สมัคร (signup_date) ใหม่ที่สุดเอาไว้เป็นข้อมูลจริง

ปรับมาตรฐานเบอร์โทร: ลบสิ่งที่ไม่ใช่ตัวเลขออกให้หมดด้วยคำสั่ง re.sub(r"\D", "", phone) ตัวอักษรที่ปนมา (เช่น "DINO") จะถูกตัดทิ้งเลย ไม่เอาไปแปลงเป็นเลข ส่วนเบอร์ไหนที่ว่างมาตั้งแต่แรก ก็ปล่อยให้ว่าง (NULL) ต่อไป (เพราะโจทย์สั่งให้เติมค่าทดแทนเฉพาะอีเมล ไม่ได้สั่งให้เติมเบอร์โทร)

การแปลงสกุลเงิน: ออเดอร์จะถูกแปลงค่าเป็น USD ก็ต่อเมื่อ มีข้อมูลสกุลเงินระบุไว้, สกุลเงินนั้นไม่ใช่ "USD", และต้องมีข้อมูลวันที่กับสกุลเงินนั้นจับคู่เจอในตารางเรทราคาพอดี ส่วนเคสอื่น ๆ นอกเหนือจากนี้จะปล่อยผ่านโดยไม่มีการแปลงค่าใด ๆ

## Part 3: Unit Testing (Python)

การเขียนชุดทดสอบไว้ในไฟล์ test_pipeline.py เพื่อทวนสอบฟังก์ชันใน transforms.py โดยไม่ต้องเชื่อมฐานข้อมูลจริง

Test Deduplication: ทดสอบว่าฟังก์ชันใน transforms.py สามารถเลือกเก็บข้อมูลลูกค้าเฉพาะแถวที่มี signup_date ล่าสุดและลบแถวที่ซ้ำซ้อนออกไปได้จริงTest Phone Standardization: 

ทดสอบการ clean เบอร์โทรศัพท์ ว่าระบบสามารถตัดช่องว่าง, ขีด, วงเล็บ และตัวอักษรปนอยู่ได้ (เช่น "DINO", "Ext") ออกไปจนเหลือแต่ตัวเลขล้วนได้ถูกต้องหรือไม่

Test Order Filtering: ทดสอบว่าระบบสามารถกรองเอาออเดอร์ที่มียอดขายติดลบหรือเป็นศูนย์ ออกได้ถูกต้อง

## Part 4: CLV Report

clv_report.sql จะทำหน้าที่เชื่อมตารางลูกค้า (dim_customers) เข้ากับตารางออเดอร์ (fct_orders) เพื่อคำนวณผลรวมจำนวนออเดอร์และยอดเงินรวมทั้งหมดที่ลูกค้าแต่ละคนจ่ายไป (ในสกุลเงิน USD) จากนั้นนำมาแบ่งกลุ่มลูกค้าตามเดือนที่สมัคร (customer_cohort) และเรียงลำดับจากคนที่จ่ายเงินให้เราสูงสุดลงมา สำหรับลูกค้าคนไหนที่ไม่มีออเดอร์เลย (หรือออเดอร์โดนตัดทิ้งเพราะระบบ error) จะใช้วิธี LEFT JOIN เพื่อให้ยังมีรายชื่ออยู่ในรายงาน 
โดยจะแสดงยอดรวมค่า lifetime_value_usd = 0
