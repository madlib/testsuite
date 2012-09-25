-------------------------
--madlibtestdata.ar_empty
-------------------------
DROP TABLE IF EXISTS madlibtestdata.ar_empty; 
create table madlibtestdata.ar_empty(
    trans_id int, 
    product text
);

alter table madlibtestdata.ar_empty owner to madlibtester;


---------------------------
--madlibtestdata.ar_example
---------------------------
DROP TABLE IF EXISTS madlibtestdata.ar_example; 
CREATE TABLE madlibtestdata.ar_example (
    trans_id INT, 
    product text
); 

INSERT INTO madlibtestdata.ar_example VALUES (1, '1'); 
INSERT INTO madlibtestdata.ar_example VALUES (1, '2'); 
INSERT INTO madlibtestdata.ar_example VALUES (1, '3'); 
INSERT INTO madlibtestdata.ar_example VALUES (2, '1'); 
INSERT INTO madlibtestdata.ar_example VALUES (2, '2'); 
INSERT INTO madlibtestdata.ar_example VALUES (3, '1'); 
INSERT INTO madlibtestdata.ar_example VALUES (3, '2');
INSERT INTO madlibtestdata.ar_example VALUES (4, '1'); 
INSERT INTO madlibtestdata.ar_example VALUES (4, '3'); 
INSERT INTO madlibtestdata.ar_example VALUES (5, '1');
INSERT INTO madlibtestdata.ar_example VALUES (6, '1'); 
INSERT INTO madlibtestdata.ar_example VALUES (6, '2'); 
INSERT INTO madlibtestdata.ar_example VALUES (6, '3'); 
INSERT INTO madlibtestdata.ar_example VALUES (7, '1'); 
INSERT INTO madlibtestdata.ar_example VALUES (7, '2'); 

alter table madlibtestdata.ar_example owner to madlibtester;


--------------------------------
--madlibtestdata.ar_nointersetct
--------------------------------
create table madlibtestdata.ar_nointersetct(
    trans_id int, 
    product text
);

INSERT INTO madlibtestdata.ar_nointersetct VALUES (1, '1'); 
INSERT INTO madlibtestdata.ar_nointersetct VALUES (1, '2'); 
INSERT INTO madlibtestdata.ar_nointersetct VALUES (2, '3'); 
INSERT INTO madlibtestdata.ar_nointersetct VALUES (2, '4');
INSERT INTO madlibtestdata.ar_nointersetct VALUES (3, '5');
INSERT INTO madlibtestdata.ar_nointersetct VALUES (3, '6');
INSERT INTO madlibtestdata.ar_nointersetct VALUES (4, '7');
INSERT INTO madlibtestdata.ar_nointersetct VALUES (4, '8');
INSERT INTO madlibtestdata.ar_nointersetct VALUES (5, '9');
INSERT INTO madlibtestdata.ar_nointersetct VALUES (5, '10');
INSERT INTO madlibtestdata.ar_nointersetct VALUES (6, '11');
INSERT INTO madlibtestdata.ar_nointersetct VALUES (6, '12');
INSERT INTO madlibtestdata.ar_nointersetct VALUES (7, '13');
INSERT INTO madlibtestdata.ar_nointersetct VALUES (7, '14');

alter table madlibtestdata.ar_nointersetct owner to madlibtester;


-----------------------------------
--madlibtestdata.ar_identical_tstwo
-----------------------------------
create table madlibtestdata.ar_identical_tstwo(
    transaction int, 
    item text
);

INSERT INTO madlibtestdata.ar_identical_tstwo VALUES (1, '1'); 
INSERT INTO madlibtestdata.ar_identical_tstwo VALUES (1, '2'); 
INSERT INTO madlibtestdata.ar_identical_tstwo VALUES (2, '1');
INSERT INTO madlibtestdata.ar_identical_tstwo VALUES (2, '2');
INSERT INTO madlibtestdata.ar_identical_tstwo VALUES (3, '1');
INSERT INTO madlibtestdata.ar_identical_tstwo VALUES (3, '2');
INSERT INTO madlibtestdata.ar_identical_tstwo VALUES (4, '1');
INSERT INTO madlibtestdata.ar_identical_tstwo VALUES (4, '2');
INSERT INTO madlibtestdata.ar_identical_tstwo VALUES (5, '1');
INSERT INTO madlibtestdata.ar_identical_tstwo VALUES (5, '2');
INSERT INTO madlibtestdata.ar_identical_tstwo VALUES (6, '1');
INSERT INTO madlibtestdata.ar_identical_tstwo VALUES (6, '2');
INSERT INTO madlibtestdata.ar_identical_tstwo VALUES (7, '1');
INSERT INTO madlibtestdata.ar_identical_tstwo VALUES (7, '2');

alter table madlibtestdata.ar_identical_tstwo owner to madlibtester;


-------------------------------------
--madlibtestdata.ar_identical_tsthree
-------------------------------------
create table madlibtestdata.ar_identical_tsthree(
    id int, 
    product text
);

INSERT INTO madlibtestdata.ar_identical_tsthree VALUES (1, '1'); 
INSERT INTO madlibtestdata.ar_identical_tsthree VALUES (1, '2'); 
INSERT INTO madlibtestdata.ar_identical_tsthree VALUES (1, '3');
INSERT INTO madlibtestdata.ar_identical_tsthree VALUES (2, '1');
INSERT INTO madlibtestdata.ar_identical_tsthree VALUES (2, '2'); 
INSERT INTO madlibtestdata.ar_identical_tsthree VALUES (2, '3');
INSERT INTO madlibtestdata.ar_identical_tsthree VALUES (3, '1');
INSERT INTO madlibtestdata.ar_identical_tsthree VALUES (3, '2'); 
INSERT INTO madlibtestdata.ar_identical_tsthree VALUES (3, '3');
INSERT INTO madlibtestdata.ar_identical_tsthree VALUES (4, '1');
INSERT INTO madlibtestdata.ar_identical_tsthree VALUES (4, '2'); 
INSERT INTO madlibtestdata.ar_identical_tsthree VALUES (4, '3');
INSERT INTO madlibtestdata.ar_identical_tsthree VALUES (5, '1');
INSERT INTO madlibtestdata.ar_identical_tsthree VALUES (5, '2'); 
INSERT INTO madlibtestdata.ar_identical_tsthree VALUES (5, '3');
INSERT INTO madlibtestdata.ar_identical_tsthree VALUES (6, '1');
INSERT INTO madlibtestdata.ar_identical_tsthree VALUES (6, '2'); 
INSERT INTO madlibtestdata.ar_identical_tsthree VALUES (6, '3');
INSERT INTO madlibtestdata.ar_identical_tsthree VALUES (7, '1');
INSERT INTO madlibtestdata.ar_identical_tsthree VALUES (7, '2'); 
INSERT INTO madlibtestdata.ar_identical_tsthree VALUES (7, '3');

alter table madlibtestdata.ar_identical_tsthree owner to madlibtester;


-----------------------------------
--madlibtestdata.ar_identical_tsten
-----------------------------------
create table madlibtestdata.ar_identical_tsten(
    transaction int, 
    productname text
);

INSERT INTO madlibtestdata.ar_identical_tsten VALUES (1, '1'); 
INSERT INTO madlibtestdata.ar_identical_tsten VALUES (1, '2'); 
INSERT INTO madlibtestdata.ar_identical_tsten VALUES (1, '3');
INSERT INTO madlibtestdata.ar_identical_tsten VALUES (1, '4');
INSERT INTO madlibtestdata.ar_identical_tsten VALUES (1, '5');
INSERT INTO madlibtestdata.ar_identical_tsten VALUES (1, '6');
INSERT INTO madlibtestdata.ar_identical_tsten VALUES (1, '7');
INSERT INTO madlibtestdata.ar_identical_tsten VALUES (1, '8');
INSERT INTO madlibtestdata.ar_identical_tsten VALUES (1, '9');
INSERT INTO madlibtestdata.ar_identical_tsten VALUES (1, '10'); 

INSERT INTO madlibtestdata.ar_identical_tsten VALUES (2, '1');
INSERT INTO madlibtestdata.ar_identical_tsten VALUES (2, '2'); 
INSERT INTO madlibtestdata.ar_identical_tsten VALUES (2, '3');
INSERT INTO madlibtestdata.ar_identical_tsten VALUES (2, '4');
INSERT INTO madlibtestdata.ar_identical_tsten VALUES (2, '5');
INSERT INTO madlibtestdata.ar_identical_tsten VALUES (2, '6');
INSERT INTO madlibtestdata.ar_identical_tsten VALUES (2, '7');
INSERT INTO madlibtestdata.ar_identical_tsten VALUES (2, '8');
INSERT INTO madlibtestdata.ar_identical_tsten VALUES (2, '9');
INSERT INTO madlibtestdata.ar_identical_tsten VALUES (2, '10');

INSERT INTO madlibtestdata.ar_identical_tsten VALUES (3, '1');
INSERT INTO madlibtestdata.ar_identical_tsten VALUES (3, '2'); 
INSERT INTO madlibtestdata.ar_identical_tsten VALUES (3, '3');
INSERT INTO madlibtestdata.ar_identical_tsten VALUES (3, '4');
INSERT INTO madlibtestdata.ar_identical_tsten VALUES (3, '5');
INSERT INTO madlibtestdata.ar_identical_tsten VALUES (3, '6');
INSERT INTO madlibtestdata.ar_identical_tsten VALUES (3, '7');
INSERT INTO madlibtestdata.ar_identical_tsten VALUES (3, '8');
INSERT INTO madlibtestdata.ar_identical_tsten VALUES (3, '9');
INSERT INTO madlibtestdata.ar_identical_tsten VALUES (3, '10'); 

INSERT INTO madlibtestdata.ar_identical_tsten VALUES (4, '1');
INSERT INTO madlibtestdata.ar_identical_tsten VALUES (4, '2'); 
INSERT INTO madlibtestdata.ar_identical_tsten VALUES (4, '3');
INSERT INTO madlibtestdata.ar_identical_tsten VALUES (4, '4');
INSERT INTO madlibtestdata.ar_identical_tsten VALUES (4, '5');
INSERT INTO madlibtestdata.ar_identical_tsten VALUES (4, '6');
INSERT INTO madlibtestdata.ar_identical_tsten VALUES (4, '7');
INSERT INTO madlibtestdata.ar_identical_tsten VALUES (4, '8');
INSERT INTO madlibtestdata.ar_identical_tsten VALUES (4, '9');
INSERT INTO madlibtestdata.ar_identical_tsten VALUES (4, '10'); 

INSERT INTO madlibtestdata.ar_identical_tsten VALUES (5, '1');
INSERT INTO madlibtestdata.ar_identical_tsten VALUES (5, '2'); 
INSERT INTO madlibtestdata.ar_identical_tsten VALUES (5, '3');
INSERT INTO madlibtestdata.ar_identical_tsten VALUES (5, '4');
INSERT INTO madlibtestdata.ar_identical_tsten VALUES (5, '5');
INSERT INTO madlibtestdata.ar_identical_tsten VALUES (5, '6');
INSERT INTO madlibtestdata.ar_identical_tsten VALUES (5, '7');
INSERT INTO madlibtestdata.ar_identical_tsten VALUES (5, '8');
INSERT INTO madlibtestdata.ar_identical_tsten VALUES (5, '9');
INSERT INTO madlibtestdata.ar_identical_tsten VALUES (5, '10'); 

INSERT INTO madlibtestdata.ar_identical_tsten VALUES (6, '1');
INSERT INTO madlibtestdata.ar_identical_tsten VALUES (6, '2'); 
INSERT INTO madlibtestdata.ar_identical_tsten VALUES (6, '3');
INSERT INTO madlibtestdata.ar_identical_tsten VALUES (6, '4');
INSERT INTO madlibtestdata.ar_identical_tsten VALUES (6, '5');
INSERT INTO madlibtestdata.ar_identical_tsten VALUES (6, '6');
INSERT INTO madlibtestdata.ar_identical_tsten VALUES (6, '7');
INSERT INTO madlibtestdata.ar_identical_tsten VALUES (6, '8');
INSERT INTO madlibtestdata.ar_identical_tsten VALUES (6, '9');
INSERT INTO madlibtestdata.ar_identical_tsten VALUES (6, '10');

INSERT INTO madlibtestdata.ar_identical_tsten VALUES (7, '1');
INSERT INTO madlibtestdata.ar_identical_tsten VALUES (7, '2'); 
INSERT INTO madlibtestdata.ar_identical_tsten VALUES (7, '3');
INSERT INTO madlibtestdata.ar_identical_tsten VALUES (7, '4');
INSERT INTO madlibtestdata.ar_identical_tsten VALUES (7, '5');
INSERT INTO madlibtestdata.ar_identical_tsten VALUES (7, '6');
INSERT INTO madlibtestdata.ar_identical_tsten VALUES (7, '7');
INSERT INTO madlibtestdata.ar_identical_tsten VALUES (7, '8');
INSERT INTO madlibtestdata.ar_identical_tsten VALUES (7, '9');
INSERT INTO madlibtestdata.ar_identical_tsten VALUES (7, '10'); 

alter table madlibtestdata.ar_identical_tsten owner to madlibtester;

