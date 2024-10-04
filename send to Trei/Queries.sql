-- Task 1:-

select * from company_address.company_table_new;
select count(*)  from company_address.address_table_new;

-- too many none values
UPDATE company_address.company_table_new
SET license1StartDate = '0000-01-01'
WHERE license1StartDate = 'None';
-- updating the remaining integers into datetime format
UPDATE company_address.company_table_new
SET license1StartDate = DATE_ADD('1899-12-30', INTERVAL ROUND(license1StartDate) DAY)
WHERE license1StartDate > 0;
-- finally changing the whole column
alter table company_address.company_table_new modify column license1StartDate DATETIME;


ALTER TABLE company_address.company_table_new
ADD CONSTRAINT `PK_corp_id` PRIMARY KEY (corp_id);


-- ALTER TABLE company_address.company_table_new
-- ADD CONSTRAINT `unq_companyName` UNIQUE (companyName(300));
-- Cannot add any other unique constraints because all other columns are null except for corp_id

ALTER TABLE company_address.address_table_new
ADD CONSTRAINT `FK_corp_id` FOREIGN KEY (`corp_id`) 
REFERENCES company_address.company_table_new (`corp_id`);

-- test insert
INSERT IGNORE INTO company_address.company_table_new (corp_id, companyName, category, categoryCode, license1StartDate)
VALUES ('11', 'Johns cleaning', "Janitorial Services", '561720', '2024-01-01 00:00:00');

-- delete
Delete from company_address.company_table_new where corp_id=11;

GRANT ALL PRIVILEGES ON company_address.* TO 'root'@'localhost';
ALTER USER 'root'@'localhost' IDENTIFIED BY 'Belman@30';
FLUSH PRIVILEGES;

SELECT user, host FROM mysql.user WHERE user = 'root';

-- Task 3:-
CREATE TABLE company_address.sync_log (
    id INT AUTO_INCREMENT PRIMARY KEY,
    table_name VARCHAR(255),       -- Stores which table was affected (company_table or address_table)
    operation_type ENUM('INSERT', 'UPDATE'),
    record_id INT,                 -- Stores the 'corp_id' from the affected table
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DELIMITER $$

-- Trigger for company_table_new INSERT
CREATE TRIGGER company_address.after_insert_trigger_company
AFTER INSERT ON company_table_new
FOR EACH ROW
BEGIN
    INSERT INTO sync_log (table_name, operation_type, record_id)
    VALUES ('company_table_new', 'INSERT', NEW.corp_id);
END$$

-- Trigger for company_table_new UPDATE
CREATE TRIGGER company_address.after_update_trigger_company
AFTER UPDATE ON company_table_new
FOR EACH ROW
BEGIN
    INSERT INTO sync_log (table_name, operation_type, record_id)
    VALUES ('company_table_new', 'UPDATE', NEW.corp_id);
END$$

-- Trigger for address_table_new INSERT
CREATE TRIGGER company_address.after_insert_trigger_address
AFTER INSERT ON address_table_new
FOR EACH ROW
BEGIN
    INSERT INTO sync_log (table_name, operation_type, record_id)
    VALUES ('address_table_new', 'INSERT', NEW.corp_id);
END$$

-- Trigger for address_table_new UPDATE
CREATE TRIGGER company_address.after_update_trigger_address
AFTER UPDATE ON address_table_new
FOR EACH ROW
BEGIN
    INSERT INTO sync_log (table_name, operation_type, record_id)
    VALUES ('address_table_new', 'UPDATE', NEW.corp_id);
END$$

DELIMITER ;
CREATE TABLE company_address.sync_log (
    id INT AUTO_INCREMENT PRIMARY KEY,
    table_name VARCHAR(255),       -- Stores which table was affected (company_table or address_table)
    operation_type ENUM('INSERT', 'UPDATE'),
    record_id INT,                 -- Stores the 'corp_id' from the affected table
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DELIMITER $$

-- Trigger for company_table_new INSERT
CREATE TRIGGER company_address.after_insert_trigger_company
AFTER INSERT ON company_table_new
FOR EACH ROW
BEGIN
    INSERT INTO sync_log (table_name, operation_type, record_id)
    VALUES ('company_table_new', 'INSERT', NEW.corp_id);
END$$

-- Trigger for company_table_new UPDATE
CREATE TRIGGER company_address.after_update_trigger_company
AFTER UPDATE ON company_table_new
FOR EACH ROW
BEGIN
    INSERT INTO sync_log (table_name, operation_type, record_id)
    VALUES ('company_table_new', 'UPDATE', NEW.corp_id);
END$$

-- Trigger for address_table_new INSERT
CREATE TRIGGER company_address.after_insert_trigger_address
AFTER INSERT ON address_table_new
FOR EACH ROW
BEGIN
    INSERT INTO sync_log (table_name, operation_type, record_id)
    VALUES ('address_table_new', 'INSERT', NEW.corp_id);
END$$

-- Trigger for address_table_new UPDATE
CREATE TRIGGER company_address.after_update_trigger_address
AFTER UPDATE ON address_table_new
FOR EACH ROW
BEGIN
    INSERT INTO sync_log (table_name, operation_type, record_id)
    VALUES ('address_table_new', 'UPDATE', NEW.corp_id);
END$$

DELIMITER ;
ALTER TABLE company_address.sync_log ADD COLUMN sync_status ENUM('SYNCHRONIZED', 'PENDING') DEFAULT NULL;

