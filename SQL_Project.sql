CREATE DATABASE manufacturing_analytics;
USE manufacturing_analytics;

CREATE TABLE customers (
    cust_code VARCHAR(20) PRIMARY KEY,
    cust_name VARCHAR(100) NOT NULL,
    buyer VARCHAR(100),
    INDEX idx_cust_name (cust_name)
);

CREATE TABLE employees (
    emp_code VARCHAR(20) PRIMARY KEY,
    emp_name VARCHAR(100) NOT NULL,
    INDEX idx_emp_name (emp_name)
);

CREATE TABLE items (
    item_code VARCHAR(50) PRIMARY KEY,
    item_name VARCHAR(200) NOT NULL,
    INDEX idx_item_name (item_name)
);

CREATE TABLE machines (
    machine_code VARCHAR(50) PRIMARY KEY,
    per_day_cost DECIMAL(10,2),
    INDEX idx_machine_code (machine_code)
);

CREATE TABLE operations (
    operation_code VARCHAR(50) PRIMARY KEY,
    operation_name VARCHAR(100) NOT NULL,
    INDEX idx_operation_name (operation_name)
);

CREATE TABLE departments (
    department_id INT AUTO_INCREMENT PRIMARY KEY,
    department_name VARCHAR(100) NOT NULL UNIQUE,
    INDEX idx_dept_name (department_name)
);

CREATE TABLE production_records (
    record_id INT AUTO_INCREMENT PRIMARY KEY,
    doc_num VARCHAR(50) NOT NULL,
    doc_date DATE NOT NULL,
    cust_code VARCHAR(20),
    emp_code VARCHAR(20),
    item_code VARCHAR(50),
    machine_code VARCHAR(50),
    operation_code VARCHAR(50),
    department_name VARCHAR(100),
    designer VARCHAR(100),
    delivery_period VARCHAR(50),
    press_qty INT DEFAULT 0,
    processed_qty INT DEFAULT 0,
    produced_qty INT DEFAULT 0,
    rejected_qty INT DEFAULT 0,
    today_manufactured_qty INT DEFAULT 0,
    total_qty INT DEFAULT 0,
    wo_qty INT DEFAULT 0,
    total_value DECIMAL(12,2) DEFAULT 0.00,
    repeat_order BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (cust_code) REFERENCES customers(cust_code),
    FOREIGN KEY (emp_code) REFERENCES employees(emp_code),
    FOREIGN KEY (item_code) REFERENCES items(item_code),
    FOREIGN KEY (machine_code) REFERENCES machines(machine_code),
    FOREIGN KEY (operation_code) REFERENCES operations(operation_code),
    
    INDEX idx_doc_num (doc_num),
    INDEX idx_doc_date (doc_date),
    INDEX idx_department (department_name),
    INDEX idx_designer (designer)
);


-- Step 1: Populate Customers table
INSERT INTO customers (cust_code, cust_name, buyer)
SELECT DISTINCT 
    `Cust Code`,
    `Cust Name`,
    `Buyer`
FROM raw_manufacturing_data 
WHERE `Cust Code` IS NOT NULL AND `Cust Code` != ''
ON DUPLICATE KEY UPDATE 
    cust_name = VALUES(cust_name),
    buyer = VALUES(buyer);

-- Step 2: Populate Employees table  
INSERT INTO employees (emp_code, emp_name)
SELECT DISTINCT 
    `EMP Code`,
    `Emp Name`
FROM raw_manufacturing_data 
WHERE `EMP Code` IS NOT NULL AND `EMP Code` != ''
ON DUPLICATE KEY UPDATE 
    emp_name = VALUES(emp_name);

-- Step 3: Populate Items table
INSERT INTO items (item_code, item_name)
SELECT DISTINCT 
    `Item Code`,
    `Item Name`
FROM raw_manufacturing_data 
WHERE `Item Code` IS NOT NULL AND `Item Code` != ''
ON DUPLICATE KEY UPDATE 
    item_name = VALUES(item_name);

-- Step 4: Populate Machines table
INSERT INTO machines (machine_code, per_day_cost)
SELECT DISTINCT 
    `Machine Code`,
    `Per day Machine Cost`
FROM raw_manufacturing_data 
WHERE `Machine Code` IS NOT NULL AND `Machine Code` != ''
ON DUPLICATE KEY UPDATE 
    per_day_cost = VALUES(per_day_cost);

-- Step 5: Populate Operations table
INSERT INTO operations (operation_code, operation_name)
SELECT DISTINCT 
    `Operation Code`,
    `Operation Name`
FROM raw_manufacturing_data 
WHERE `Operation Code` IS NOT NULL AND `Operation Code` != ''
ON DUPLICATE KEY UPDATE 
    operation_name = VALUES(operation_name);

-- Step 6: Populate Departments table
INSERT INTO departments (department_name)
SELECT DISTINCT `Department Name`
FROM raw_manufacturing_data 
WHERE `Department Name` IS NOT NULL AND `Department Name` != ''
ON DUPLICATE KEY UPDATE 
    department_name = VALUES(department_name);
    
    
-- Create comprehensive view for dashboard
CREATE OR REPLACE VIEW manufacturing_dashboard AS
SELECT 
    pr.record_id,
    pr.doc_num,
    pr.doc_date,
    c.cust_name,
    c.buyer,
    e.emp_name,
    i.item_name,
    m.machine_code,
    m.per_day_cost as machine_cost,
    op.operation_name,
    pr.department_name,
    pr.designer,
    pr.delivery_period,
    pr.press_qty,
    pr.processed_qty,
    pr.produced_qty,
    pr.rejected_qty,
    pr.today_manufactured_qty,
    pr.total_qty,
    pr.wo_qty,
    pr.total_value,
    pr.repeat_order,
    -- Calculated fields for analytics
    (pr.rejected_qty / NULLIF(pr.produced_qty, 0)) * 100 as rejection_rate_percent,
    (pr.produced_qty / NULLIF(pr.wo_qty, 0)) * 100 as production_efficiency_percent,
    pr.total_value / NULLIF(pr.total_qty, 0) as value_per_unit,
    YEAR(pr.doc_date) as production_year,
    MONTH(pr.doc_date) as production_month,
    DAYNAME(pr.doc_date) as production_day_name
FROM production_records pr
LEFT JOIN customers c ON pr.cust_code = c.cust_code
LEFT JOIN employees e ON pr.emp_code = e.emp_code
LEFT JOIN items i ON pr.item_code = i.item_code
LEFT JOIN machines m ON pr.machine_code = m.machine_code
LEFT JOIN operations op ON pr.operation_code = op.operation_code;

-- Create quality metrics view
CREATE OR REPLACE VIEW quality_metrics AS
SELECT 
    c.cust_name,
    i.item_name,
    op.operation_name,
    pr.department_name,
    SUM(pr.produced_qty) as total_produced,
    SUM(pr.rejected_qty) as total_rejected,
    AVG(pr.rejected_qty / NULLIF(pr.produced_qty, 0)) * 100 as avg_rejection_rate,
    COUNT(*) as total_orders
FROM production_records pr
LEFT JOIN customers c ON pr.cust_code = c.cust_code
LEFT JOIN items i ON pr.item_code = i.item_code
LEFT JOIN operations op ON pr.operation_code = op.operation_code
GROUP BY c.cust_name, i.item_name, op.operation_name, pr.department_name;

-- Create production summary view
CREATE OR REPLACE VIEW production_summary AS
SELECT 
    DATE(pr.doc_date) as production_date,
    pr.department_name,
    e.emp_name,
    SUM(pr.produced_qty) as daily_production,
    SUM(pr.rejected_qty) as daily_rejections,
    SUM(pr.total_value) as daily_value,
    COUNT(DISTINCT pr.item_code) as unique_items_produced
FROM production_records pr
LEFT JOIN employees e ON pr.emp_code = e.emp_code
GROUP BY DATE(pr.doc_date), pr.department_name, e.emp_name;

-- Create machine utilization view
CREATE OR REPLACE VIEW machine_utilization AS
SELECT 
    m.machine_code,
    m.per_day_cost,
    op.operation_name,
    COUNT(*) as total_operations,
    SUM(pr.produced_qty) as total_units_produced,
    SUM(pr.total_value) as total_value_generated,
    AVG(pr.produced_qty) as avg_units_per_operation
FROM production_records pr
LEFT JOIN machines m ON pr.machine_code = m.machine_code
LEFT JOIN operations op ON pr.operation_code = op.operation_code
GROUP BY m.machine_code, m.per_day_cost, op.operation_name;


-- Alternative with different date format
INSERT INTO production_records (
    doc_num,
    doc_date,
    cust_code,
    emp_code,
    item_code,
    machine_code,
    operation_code,
    department_name,
    designer,
    delivery_period,
    press_qty,
    processed_qty,
    produced_qty,
    rejected_qty,
    today_manufactured_qty,
    total_qty,
    wo_qty,
    total_value,
    repeat_order
)
SELECT 
    `Doc Num`,
    DATE(`Doc Date`),
    `Cust Code`,
    `EMP Code`,
    `Item Code`,
    `Machine Code`,
    `Operation Code`,
    `Department Name`,
    `Designer`,
    `Delivery Period`,
    COALESCE(`Press Qty`, 0),
    COALESCE(`Processed Qty`, 0),
    COALESCE(`Produced Qty`, 0),
    COALESCE(`Rejected Qty`, 0),
    COALESCE(`today Manufactured qty`, 0),
    COALESCE(`TotalQty`, 0),
    COALESCE(`WO Qty`, 0),
    COALESCE(`TotalValue`, 0.00),
    CASE 
        WHEN `Repeat` = 1 THEN 1 
        ELSE 0 
    END
FROM raw_manufacturing_data
WHERE `Doc Num` IS NOT NULL 
    AND `Doc Num` != ''
    AND `Doc Date` IS NOT NULL
    AND `Doc Date` != '';
    
    
-- Create the main dashboard view (run this first)
CREATE OR REPLACE VIEW manufacturing_dashboard AS
SELECT 
    pr.record_id,
    pr.doc_num,
    pr.doc_date,
    c.cust_name,
    c.buyer,
    e.emp_name,
    i.item_name,
    m.machine_code,
    m.per_day_cost as machine_cost,
    op.operation_name,
    pr.department_name,
    pr.designer,
    pr.delivery_period,
    pr.press_qty,
    pr.processed_qty,
    pr.produced_qty,
    pr.rejected_qty,
    pr.today_manufactured_qty,
    pr.total_qty,
    pr.wo_qty,
    pr.total_value,
    pr.repeat_order,
    -- Calculated KPIs for dashboard
    CASE 
        WHEN pr.produced_qty > 0 THEN (pr.rejected_qty / pr.produced_qty) * 100 
        ELSE 0 
    END as rejection_rate_percent,
    CASE 
        WHEN pr.wo_qty > 0 THEN (pr.produced_qty / pr.wo_qty) * 100 
        ELSE 0 
    END as production_efficiency_percent,
    CASE 
        WHEN pr.total_qty > 0 THEN pr.total_value / pr.total_qty 
        ELSE 0 
    END as value_per_unit,
    YEAR(pr.doc_date) as production_year,
    MONTH(pr.doc_date) as production_month,
    MONTHNAME(pr.doc_date) as production_month_name,
    DAYNAME(pr.doc_date) as production_day_name
FROM production_records pr
LEFT JOIN customers c ON pr.cust_code = c.cust_code
LEFT JOIN employees e ON pr.emp_code = e.emp_code
LEFT JOIN items i ON pr.item_code = i.item_code
LEFT JOIN machines m ON pr.machine_code = m.machine_code
LEFT JOIN operations op ON pr.operation_code = op.operation_code;

-- Key Performance Indicators View
CREATE OR REPLACE VIEW manufacturing_kpis AS
SELECT 
    COUNT(*) as total_orders,
    COUNT(DISTINCT cust_code) as unique_customers,
    COUNT(DISTINCT item_code) as unique_items,
    COUNT(DISTINCT emp_code) as active_employees,
    SUM(produced_qty) as total_production,
    SUM(rejected_qty) as total_rejections,
    SUM(total_value) as total_revenue,
    AVG(CASE WHEN produced_qty > 0 THEN (rejected_qty/produced_qty)*100 ELSE 0 END) as avg_rejection_rate,
    AVG(CASE WHEN wo_qty > 0 THEN (produced_qty/wo_qty)*100 ELSE 0 END) as avg_efficiency
FROM production_records;

-- Daily Production Summary
CREATE OR REPLACE VIEW daily_production_summary AS
SELECT 
    DATE(doc_date) as production_date,
    department_name,
    COUNT(*) as orders_processed,
    SUM(produced_qty) as daily_production,
    SUM(rejected_qty) as daily_rejections,
    SUM(total_value) as daily_revenue,
    COUNT(DISTINCT emp_code) as employees_active
FROM production_records
GROUP BY DATE(doc_date), department_name
ORDER BY production_date DESC;

-- Customer Analysis View
CREATE OR REPLACE VIEW customer_analysis AS
SELECT 
    c.cust_name,
    c.buyer,
    COUNT(*) as total_orders,
    SUM(pr.total_value) as total_revenue,
    SUM(pr.produced_qty) as total_units,
    AVG(CASE WHEN pr.produced_qty > 0 THEN (pr.rejected_qty/pr.produced_qty)*100 ELSE 0 END) as avg_rejection_rate,
    MAX(pr.doc_date) as last_order_date,
    COUNT(DISTINCT pr.item_code) as unique_items_ordered
FROM customers c
LEFT JOIN production_records pr ON c.cust_code = pr.cust_code
GROUP BY c.cust_code, c.cust_name, c.buyer
ORDER BY total_revenue DESC;


-- Add primary keys to tables (run one by one)
ALTER TABLE customers ADD COLUMN id INT AUTO_INCREMENT PRIMARY KEY FIRST;
ALTER TABLE departments ADD COLUMN id INT AUTO_INCREMENT PRIMARY KEY FIRST;
ALTER TABLE employees ADD COLUMN id INT AUTO_INCREMENT PRIMARY KEY FIRST;
ALTER TABLE items ADD COLUMN id INT AUTO_INCREMENT PRIMARY KEY FIRST;
ALTER TABLE machines ADD COLUMN id INT AUTO_INCREMENT PRIMARY KEY FIRST;
ALTER TABLE operations ADD COLUMN id INT AUTO_INCREMENT PRIMARY KEY FIRST;
ALTER TABLE production_records ADD COLUMN id INT AUTO_INCREMENT PRIMARY KEY FIRST;



-- Create indexes on frequently used columns (skip any that give errors)
CREATE INDEX idx_cust_code ON customers(cust_code);
CREATE INDEX idx_emp_code ON employees(emp_code);  
CREATE INDEX idx_machine_code ON machines(machine_code);
CREATE INDEX idx_item_code ON items(item_code);
CREATE INDEX idx_operation_name ON operations(operation_name);

-- Create indexes on production_records for joins
CREATE INDEX idx_prod_cust_code ON production_records(cust_code);
CREATE INDEX idx_prod_emp_code ON production_records(emp_code);
CREATE INDEX idx_prod_machine_code ON production_records(machine_code);
CREATE INDEX idx_prod_item_code ON production_records(item_code);
CREATE INDEX idx_prod_date ON production_records(doc_date);


-- Create customer summary
CREATE OR REPLACE VIEW customer_summary AS
SELECT 
    pr.cust_code,
    COUNT(*) as total_orders,
    SUM(pr.produced_qty) as total_production,
    SUM(pr.rejected_qty) as total_rejected,
    SUM(pr.total_value) as total_business_value,
    AVG(pr.total_value) as avg_order_value
FROM production_records pr
GROUP BY pr.cust_code;


-- Create daily production summary
CREATE OR REPLACE VIEW daily_production_summary AS
SELECT 
    DATE(doc_date) as production_date,
    cust_code,
    department_name,
    COUNT(*) as total_records,
    SUM(produced_qty) as total_produced,
    SUM(rejected_qty) as total_rejected,
    SUM(today_manufactured_qty) as daily_manufactured,
    SUM(total_value) as total_value,
    AVG(total_value) as avg_value
FROM production_records
WHERE doc_date IS NOT NULL
GROUP BY DATE(doc_date), cust_code, department_name;


-- Monthly Production Trends
CREATE OR REPLACE VIEW monthly_production_trends AS
SELECT 
    YEAR(doc_date) as year,
    MONTH(doc_date) as month,
    MONTHNAME(doc_date) as month_name,
    COUNT(*) as total_operations,
    SUM(produced_qty) as total_produced,
    SUM(rejected_qty) as total_rejected,
    SUM(total_value) as monthly_revenue,
    ROUND(AVG(total_value), 2) as avg_order_value
FROM production_records
WHERE doc_date IS NOT NULL
GROUP BY YEAR(doc_date), MONTH(doc_date), MONTHNAME(doc_date)
ORDER BY year, month;


-- Department Performance
CREATE OR REPLACE VIEW department_performance AS
SELECT 
    department_name,
    COUNT(*) as total_operations,
    COUNT(DISTINCT cust_code) as unique_customers,
    COUNT(DISTINCT emp_code) as unique_employees,
    SUM(produced_qty) as total_produced,
    SUM(rejected_qty) as total_rejected,
    ROUND((SUM(rejected_qty) / NULLIF(SUM(produced_qty + rejected_qty), 0)) * 100, 2) as rejection_rate_percent,
    SUM(total_value) as department_revenue
FROM production_records
WHERE department_name IS NOT NULL
GROUP BY department_name;


-- Quality Control Analysis
CREATE OR REPLACE VIEW quality_analysis AS
SELECT 
    doc_date,
    cust_code,
    department_name,
    machine_code,
    operation_code,
    produced_qty,
    rejected_qty,
    CASE 
        WHEN (produced_qty + rejected_qty) > 0 
        THEN ROUND((rejected_qty / (produced_qty + rejected_qty)) * 100, 2)
        ELSE 0
    END as rejection_rate_percent,
    total_value,
    CASE 
        WHEN rejected_qty = 0 THEN 'Perfect'
        WHEN (rejected_qty / NULLIF(produced_qty + rejected_qty, 0)) <= 0.05 THEN 'Excellent'
        WHEN (rejected_qty / NULLIF(produced_qty + rejected_qty, 0)) <= 0.10 THEN 'Good'
        WHEN (rejected_qty / NULLIF(produced_qty + rejected_qty, 0)) <= 0.20 THEN 'Needs Improvement'
        ELSE 'Critical'
    END as quality_grade
FROM production_records
WHERE produced_qty > 0 OR rejected_qty > 0;


-- Create a new user for your teammate
CREATE USER 'teammate01'@'%' IDENTIFIED BY '12345';

-- Grant permissions to specific database
GRANT SELECT ON manufacturing_analytics.* TO 'teammate01'@'%';

-- Apply changes
FLUSH PRIVILEGES;