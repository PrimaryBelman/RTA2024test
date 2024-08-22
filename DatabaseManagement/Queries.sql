select * from company_address.company_table_new;
select * from company_address.address_table_new;

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