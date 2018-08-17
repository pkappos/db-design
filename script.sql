use projecta;

CREATE INDEX camount ON collateral(collateral_amount);

CREATE INDEX bvalue ON balance(balance_value);


CREATE TABLE dat_table (
    customer_id VARCHAR(9),
    birth_date DATE,
    afm VARCHAR(9),
    contract_id VARCHAR(14),
    signature_date DATE,
    limit_amount DECIMAL(10 , 2 ),
    contract_type VARCHAR(17),
    account_id VARCHAR(15),
    starting_date DATE,
    status VARCHAR(13),
    product_code VARCHAR(4),
    collateral_id VARCHAR(10),
    collateral_type INT,
    collateral_amount DECIMAL(10 , 2 ),
    collateral_end DATE,
    collateral_relation_type INT,
    real_estate_id VARCHAR(10),
    appreciation_value DECIMAL(10 , 2 ),
    appreciation_date DATE,
    property_type INT
);

CREATE TABLE bal_table (
    customer_id VARCHAR(9),
    birth_date DATE,
    afm VARCHAR(9),
    contract_id VARCHAR(14),
    signature_date DATE,
    limit_amount DECIMAL(10 , 2 ),
    contract_type VARCHAR(17),
    account_id VARCHAR(15),
    starting_date DATE,
    status VARCHAR(13),
    product_code VARCHAR(4),
    balance_value DECIMAL(10 , 2 ),
    balance_date DATE,
    balance_type VARCHAR(8)
);

CREATE TABLE IF NOT EXISTS customer (
    customer_id INTEGER NOT NULL AUTO_INCREMENT,
    birth_date VARCHAR(50),
    afm INTEGER,
    contract_id INTEGER,
    PRIMARY KEY (customer_id)
);
 
CREATE TABLE IF NOT EXISTS balance (
    balance_value INTEGER,
    balance_date VARCHAR(50),
    balance_type VARCHAR(10),
    account_id INTEGER,
    FOREIGN KEY (account_id)
        REFERENCES Accounts (account_id)
);
 
CREATE TABLE IF NOT EXISTS account (
    account_id INTEGER NOT NULL AUTO_INCREMENT,
    starting_date VARCHAR(50),
    status VARCHAR(50),
    product_code INTEGER,
    contract_id INTEGER,
    PRIMARY KEY (account_id),
    FOREIGN KEY (contract_id)
        REFERENCES Contracts (contract_id)
);
 
CREATE TABLE IF NOT EXISTS contract (
    contract_id VARCHAR(100) NOT NULL,
    signature_date VARCHAR(50),
    limit_amount INTEGER,
    contract_type VARCHAR(50),
    customer_id INTEGER,
    PRIMARY KEY (contract_id),
    FOREIGN KEY (customer_id)
        REFERENCES Customers (customer_id)
);
 
CREATE TABLE IF NOT EXISTS collateral (
    collateral_id INTEGER NOT NULL AUTO_INCREMENT,
    collateral_type INTEGER,
    collateral_amount INTEGER,
    collateral_end VARCHAR(50),
    collateral_relation_type INTEGER,
    customer_id INTEGER,
    account_id INTEGER,
    contract_id VARCHAR(100),
    PRIMARY KEY (collateral_id),
    FOREIGN KEY (customer_id)
        REFERENCES Customers (customer_id),
    FOREIGN KEY (account_id)
        REFERENCES Accounts (account_id),
    FOREIGN KEY (contract_id)
        REFERENCES Contracts (contract_id)
);
 
CREATE TABLE IF NOT EXISTS real_estate (
    real_estate_id INTEGER NOT NULL AUTO_INCREMENT,
    appreciation_value INTEGER,
    appreciation_date VARCHAR(50),
    property_type INTEGER,
    collateral_id INTEGER,
    PRIMARY KEY (real_estate_id),
    FOREIGN KEY (collateral_id)
        REFERENCES Collaterals (collateral_id)
);

CREATE TABLE IF NOT EXISTS balance (
    balance_value DECIMAL,
    balance_date DATE,
    balance_type VARCHAR(20),
    account_id INTEGER,
    FOREIGN KEY (account_id)
        REFERENCES account (account_id)
);

-- LOAD DATA TO TEMPORARY TABLES

load data local infile 'C:/Data/dat2.txt'
into table dat_table 
fields terminated by '@' 
lines terminated by '\r\n'
ignore 1 rows
(customer_id, @var1, afm, contract_id, @var2, limit_amount, contract_type, account_id, @var3, status, product_code, collateral_id, collateral_type, collateral_amount, @var4, collateral_relation_type, real_estate_id, appreciation_value, @var5, property_type)
set
birth_date = STR_TO_DATE(@var1, '%d/%m/%Y'),
signature_date = STR_TO_DATE(@var2, '%d/%m/%Y'),
starting_date = STR_TO_DATE(@var3, '%d/%m/%Y'),
collateral_end = STR_TO_DATE(@var4, '%d/%m/%Y'),
appreciation_date = STR_TO_DATE(@var5, '%d/%m/%Y');

load data local infile 'C:/Data/bal2.txt'
into table bal_table 
fields terminated by '@' 
lines terminated by '\r\n'
ignore 1 rows
(customer_id, @var1, afm, contract_id, @var2, limit_amount, contract_type, account_id, @var3, status, product_code, balance_value, @var4, balance_type)
set
birth_date = STR_TO_DATE(@var1, '%d/%m/%Y'),
signature_date = STR_TO_DATE(@var2, '%d/%m/%Y'),
starting_date = STR_TO_DATE(@var3, '%d/%m/%Y'),
balance_date = STR_TO_DATE(@var4, '%d/%m/%Y');

-- INSERT THE NEW DATA TO THE  TABLES

insert into customer
select distinct customer_id, birth_date, AFM
from dat_table
	where customer_id not in (
		select i.customer_id
        from dat_table as i, customer as a
        where i.customer_id = a.customer_id
    );

insert into contract
select distinct contract_id, signature_date, limit_amount, contract_type, customer_id
from dat_table
	where contract_id not in (
		select i.contract_id
        from dat_table as i, contract as a
        where i.contract_id = a.contract_id
    );

insert into account
select distinct account_id, starting_date, status, product_code,  contract_id
from dat_table
	where account_id not in (
		select i.account_id
        from dat_table as i, account as a
        where i.account_id = a.account_id
    );

insert into collateral
select distinct collateral_id, collateral_type, collateral_amount, 
collateral_end, collateral_relation_type, 
case when collateral_relation_type=1 then
customer_id end, 
case when collateral_relation_type=2 then
contract_id end,
case when collateral_relation_type=3 then
account_id end 
from dat_table
	where collateral_id not in (
		select i.collateral_id
        from dat_table as i, collateral as a
        where i.collateral_id = a.collateral_id
    );


insert into real_estate
select distinct real_estate_id, appreciation_value, appreciation_date, property_type, collateral_id
from dat_table
where real_estate_id not in (
	select i.real_estate_id
    from dat_table as i, real_estate as a
    where i.real_estate_id = a.real_estate_id
);

insert into balance
select distinct account_id, balance_value, balance_date, balance_type
from bal_table;


-- START THE QUERIES
/* QUERY 1*/
SELECT 
    COUNT(*)
FROM
    (SELECT 
        account.account_id, MAX(balance_value) mmxx, COUNT(*)
    FROM
        account 
    INNER JOIN balance force index(bvalue) ON account.account_id = balance.account_id
    GROUP BY account_id) t1
        JOIN
    (SELECT 
        COUNT(collateral.collateral_amount),
            SUM(collateral.collateral_amount) sscc,
            account_id
    FROM
        collateral FORCE INDEX (camount) -- force 
    GROUP BY account_id) t2 ON t1.account_id = t2.account_id
WHERE
    t1.mmxx > t2.sscc;
    /* QUERY 2 */
SELECT 
    AVG(balance_value), balance.*
FROM
    (SELECT 
        MAX(balance_date) maxdate, account_id
    FROM
        balance FORCE INDEX (bvalue)
    WHERE
        balance_date <= '2006/01/01'
    GROUP BY balance.account_id) AS t1
        JOIN
    balance ON balance.account_id = t1.account_id
        AND balance.balance_date = t1.maxdate;
        /* QUERY 3 */
        
SELECT 
    COUNT(collateral_id)
FROM
    (SELECT 
        customer.birth_date,
            collateral.collateral_amount,
            contract.contract_id,
            collateral.collateral_id
    FROM
        collateral FORCE INDEX (camount)
    INNER JOIN contract ON collateral.contract_id = contract.contract_id
    INNER JOIN customer ON contract.customer_id = customer.customer_id) AS t1
        JOIN
    (SELECT 
        MAX(balance_value) mm, contract.contract_id
    FROM
        contract 
    INNER JOIN account ON contract.contract_id = account.contract_id
    INNER JOIN balance force index(bvalue) ON account.account_id = balance.account_id
    GROUP BY contract_id) AS t2 ON t1.contract_id = t2.contract_id
WHERE
    t1.birth_date <= '1957/01/01'
        AND t1.collateral_amount < 1000000
        AND t2.mm < 500000;

/* QUERY 4 */


SELECT 
    COUNT(t1.account_id)
FROM
    (SELECT 
        collateral.account_id
    FROM
        collateral force index(camount)
    INNER JOIN real_estate ON collateral.collateral_id = real_estate.collateral_id
    WHERE
        appreciation_value > 100000) AS t1
        JOIN
    (SELECT 
        MAX(balance_value) mm, account.account_id
    FROM
        account 
    INNER JOIN balance force index(bvalue) ON account.account_id = balance.account_id
    WHERE
        balance_type = 'Interest'
    GROUP BY account_id) AS t2 ON t1.account_id = t2.account_id
WHERE
    t2.mm < 500000;
    

