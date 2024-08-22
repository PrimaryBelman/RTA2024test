select * from company_address.company_table;
select * from company_address.address_table_new;
alter table company_address.company_table modify column license1StartDate DATETIME;
ALTER TABLE company_address.company_table
ADD CONSTRAINT `PK_corp_id` PRIMARY KEY (corp_id);


ALTER TABLE company_address.address_table_new
ADD CONSTRAINT `FK_corp_id` FOREIGN KEY (`corp_id`) 
REFERENCES company_addressaddress_table_new.company_table (`corp_id`);

insert into company_address.company_table values('','','','','');
