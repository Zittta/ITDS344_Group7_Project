# โครงสร้างข้อมูลและคู่มือระบบ (Data Structure & Guide)
**ITDS344 Group 7 — Seattle Public Safety Data Lake**

---

## ภาพรวมสถาปัตยกรรม (Architecture Overview)

ระบบบริหารจัดการข้อมูลโดยใช้หลักการ **Medallion Architecture** แบ่งเป็น 3 ระดับหลัก (Bronze, Silver, Gold) ผ่าน 3 Pipeline อิสระ:

1. **Pipeline 1: Seattle 911** (ความถี่ 5 นาที)
   - จัดการข้อมูลการรับแจ้งเหตุ 911 แบบใกล้เคียงเวลาจริง (Near Real-time)
   - มีการทำ Geo-Enrichment เพื่อระบุชื่อย่าน (Neighborhood) จากพิกัด Lat/Lon

2. **Pipeline 2: SPD Crime** (ความถี่ 60 นาที)
   - จัดการข้อมูลรายงานคดีอาญาจากตำรวจเมืองซีแอตเทิล (SPD)
   - สร้างตารางมิติ (Dimension Tables) และการสรุปผลเชิงวิเคราะห์ข้ามชุดข้อมูล (Cross-dataset)

3. **Pipeline 3: Population** (รันเมื่อเริ่มต้นระบบ)
   - จัดการข้อมูลประชากรจากสำมะโนประชากร (ACS Census) เพื่อใช้เป็นฐานในการคำนวณอัตราส่วน

---

## ชั้นข้อมูล Bronze (Raw Layer)
พื้นที่จัดเก็บข้อมูลดิบ (Immutable) ที่คงรูปแบบดั้งเดิมจากแหล่งข้อมูล

### `bronze.seattle_911`
- **แหล่งข้อมูล:** Socrata API (911 Dispatch)
- **คีย์หลัก:** `incident_number`
- **ข้อมูลสำคัญ:** ประเภทเหตุการณ์ (`type`), วันเวลา (`datetime`), พิกัด และตำแหน่งที่อยู่

### `bronze.spd_crime`
- **แหล่งข้อมูล:** Socrata API (SPD Crime Reports)
- **คีย์หลัก:** `offense_id`
- **ข้อมูลสำคัญ:** ประเภทความผิด (`offense_category`), รหัส NIBRS, สถานที่เกิดเหตุ (Precinct/Sector/Beat) และพิกัด

### `bronze.seattle_population`
- **แหล่งข้อมูล:** ACS Census (ไฟล์ CSV)
- **คีย์หลัก:** `(neighborhood_name, acs_year)`
- **ข้อมูลสำคัญ:** จำนวนประชากรทั้งหมด, โครงสร้างรายได้, ความยากจน และอายุขัยเฉลี่ย

---

## ชั้นข้อมูล Silver (Cleaned Layer)
ข้อมูลที่ผ่านกระบวนการตรวจสอบคุณภาพ (Data Quality) และปรับรูปแบบให้เป็นมาตรฐานเดียวกัน

- **กฎการตรวจสอบ (DQ Rules):**
  - กรองข้อมูลที่เป็นค่าว่าง (Null) ในฟิลด์ที่จำเป็น
  - ตรวจสอบความถูกต้องของพิกัดภูมิศาสตร์ (Latitude/Longitude)
  - แปลงวันที่และเวลาให้เป็นรูปแบบ ISODate (UTC)
  - กำจัดข้อมูลที่ซ้ำซ้อน (Deduplication) โดยใช้ MongoDB Upsert

---

## ชั้นข้อมูล Gold (Business Layer)
ข้อมูลที่พร้อมสำหรับการวิเคราะห์ในรูปแบบ Star Schema และตารางสรุปผล (Aggregations)

### ตารางมิติ (Dimension Tables)
- **`gold.dim_location`**: รวบรวมข้อมูลสถานที่ ตำรวจเขต (Precinct) และสายตรวจ (Beat)
- **`gold.dim_offense`**: มาตรฐานประเภทความผิดตามหลักสากล NIBRS (FBI)
- **`gold.dim_neighborhood`**: ศูนย์กลางข้อมูลย่าน (Hub) ที่เชื่อมต่อข้อมูลอาชญากรรม 911 และประชากรเข้าด้วยกัน

### ตารางข้อเท็จจริง (Fact Tables)
- **`gold.fact_crime_events`**: รายละเอียดคดีอาญาแต่ละคดี พร้อม FK เชื่อมไปยังตารางมิติ
- **`gold.fact_911_calls`**: รายละเอียดการแจ้งเหตุ 911 ที่ผ่านการระบุชื่อย่าน (Enriched) แล้ว

### ตารางสรุปผลเชิงวิเคราะห์ (Aggregation Collections)
- **`gold.agg_neighborhood_safety_profile`**: ข้อมูลสรุปความปลอดภัยระดับย่าน (สรุปจบใน 1 แถว) ประกอบด้วย อัตราคดีอาญา, อัตราการแจ้งเหตุ 911 และข้อมูลประชากร
- **`gold.agg_crime_per_capita`**: อัตราอาชญากรรมต่อประชากร 10,000 คน
- **`gold.agg_crime_trend_monthly`**: แนวโน้มอาชญากรรมรายเดือนแยกตามพื้นที่และประเภท
- **`gold.agg_911_by_hour_day`**: สถิติการแจ้งเหตุ 911 แยกตามชั่วโมงและวันในสัปดาห์ (Heatmap)

---

## สถานะคลังข้อมูล (Data Inventory Status)
ข้อมูลอัปเดตล่าสุด ณ วันที่ 2026-04-28:

| ฐานข้อมูล | คอลเลกชัน | สถานะ | หมายเหตุ |
|----------|-----------|--------|---------|
| Bronze | 3 รายการ | ปกติ | ครอบคลุม 911, Crime, Population |
| Silver | 3 รายการ | ปกติ | ข้อมูลผ่านการ Clean และจัดระเบียบแล้ว |
| Gold | 12 รายการ | ปกติ | พร้อมใช้งานสำหรับการวิเคราะห์และ Dashboard |

---

## คู่มือการเข้าถึงข้อมูลและเครื่องมือ

### แผงควบคุม (Control Center)
- **Airflow (http://localhost:8080)**: ใช้สำหรับสั่งรัน Pipeline (Manual Trigger) และดู Logs การทำงาน
- **Dashboard (http://localhost:8501)**: ใช้สำหรับดูการวิเคราะห์ผลลัพธ์เชิงภาพ (Visualizations)

### ลำดับการรัน Pipeline ที่แนะนำ (กรณีเริ่มระบบใหม่)
1. **seattle_population_pipeline**: เพื่อสร้างพื้นฐานข้อมูลประชากร
2. **spd_crime_pipeline**: เพื่อสร้างโครงสร้างย่าน (Dimension)
3. **seattle_911_pipeline**: เพื่อนำข้อมูล 911 เข้าและผูกเข้ากับชื่อย่าน
4. **spd_crime_pipeline (รันซ้ำ)**: เพื่ออัปเดตตารางสรุปผล Safety Profile ให้มีข้อมูล 911 ครบถ้วน

---

## มาตรฐานการจัดกลุ่มอาชญากรรม (NIBRS)
ระบบใช้มาตรฐาน FBI NIBRS ในการจำแนกประเภทความผิด:
- **Crimes Against PERSON**: กระทำต่อบุคคล (เช่น การทำร้ายร่างกาย)
- **Crimes Against PROPERTY**: กระทำต่อทรัพย์สิน (เช่น การลักทรัพย์)
- **Crimes Against SOCIETY**: กระทำต่อระเบียบสังคม (เช่น ความผิดเกี่ยวกับยาเสพติดหรืออาวุธ)
