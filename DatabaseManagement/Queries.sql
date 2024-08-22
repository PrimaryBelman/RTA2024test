select * from company_address.company_table_new;
select * from company_address.address_table_new;
UPDATE company_address.company_table_new
SET license1StartDate = '0000-01-01'
WHERE license1StartDate = 'None';

UPDATE company_address.company_table_new
SET license1StartDate = DATE_ADD('1899-12-30', INTERVAL ROUND(license1StartDate) DAY)
WHERE license1StartDate > 0;

alter table company_address.company_table_new modify column license1StartDate DATETIME;
ALTER TABLE company_address.company_table_new
ADD CONSTRAINT `PK_corp_id` PRIMARY KEY (corp_id);


ALTER TABLE company_address.address_table_new
ADD CONSTRAINT `FK_corp_id` FOREIGN KEY (`corp_id`) 
REFERENCES company_address.company_table_new (`corp_id`);


insert into company_address.company_table values('','','','','');
