CREATE TABLE companies(
   company_id varchar(24) NOT NULL,
   company_name varchar(130) NULL,
   PRIMARY KEY (company_id)
);

CREATE TABLE charges(
   id varchar(24) NOT NULL,
   company_id varchar(24) NOT NULL,
   amount decimal(16,2) NOT NULL,
   status varchar(30) NOT NULL,
   created_at timestamp NOT NULL,
   updated_at timestamp NULL,
   PRIMARY KEY (id)
   CONSTRAINT fk_companies
      FOREIGN KEY(company_id) 
	  REFERENCES companies(company_id)
);

CREATE VIEW total_amount AS
SELECT companies.company_id, companies.company_name, charges.created_at, SUM(charges.amount)
FROM companies
INNER JOIN charges
ON charges.company_id = companies.company_id
GROUP BY companies.company_id, companies.company_name, charges.created_at;
