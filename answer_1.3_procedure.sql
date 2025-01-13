/*
DO $$
declare 
my_date date := '01-02-2018'; 
begin

insert into logs.logs_ds
(etl_table, date_start, operation_status)
values ('dm_f101_round_f',clock_timestamp()::TIME,9);

call DM.FILL_F101_ROUND_F(my_date);

UPDATE logs.logs_ds 
SET
DATE_END = NOW()::TIME,
OPERATION_STATUS = 0,
TIME_ETL = clock_timestamp()::TIME - DATE_START
WHERE
OPERATION_STATUS = 9;

end $$ LANGUAGE PLPGSQL;

---
SELECT * FROM DM.DM_F101_ROUND_F;

DELETE FROM DM.DM_F101_ROUND_F;
*/

CREATE
OR REPLACE PROCEDURE DM.FILL_F101_ROUND_F (ION_DATE DATE) AS $$
begin
create table table_1 as 
select 
(ION_DATE - interval '1 month')::date as from_date,
(ION_DATE  - interval '1 day')::date as to_date,
"CHAPTER",
"LEDGER_ACCOUNT",
acc."CHAR_TYPE" as CHARACTERISTIC,
sum(acc_b.balance_out_rub) FILTER(where acc."CURRENCY_CODE" = '810' or acc."CURRENCY_CODE" = '643') as BALANCE_IN_RUB,
sum(acc_b.balance_out_rub) FILTER(where acc."CURRENCY_CODE" <> '810' and acc."CURRENCY_CODE" <> '643') as BALANCE_IN_VAL,
sum(acc_b.balance_out_rub) as BALANCE_IN_TOTAL 
from DS.MD_LEDGER_ACCOUNT_S as ldg
join ds.md_account_d as acc on ldg."LEDGER_ACCOUNT" = left(acc."ACCOUNT_NUMBER",5)::integer
--берём записи баланса за день предшедствующий отчётному периоду
left join dm.dm_account_balance_f as acc_b on acc_b.on_date = (((ION_DATE - interval '1 month') - interval '1 day')::date)  
and acc_b.account_rk = acc."ACCOUNT_RK"
group by ldg."LEDGER_ACCOUNT","CHAPTER",acc."CHAR_TYPE";


---------for DEBET,CREDIT,BALANCE end-----------
create table table_2 as
select 
"LEDGER_ACCOUNT",
sum(acc_tur.debet_amount_rub) FILTER(where acc."CURRENCY_CODE" = '810' or acc."CURRENCY_CODE" = '643') as TURN_DEB_RUB ,
sum(acc_tur.debet_amount_rub) FILTER(where acc."CURRENCY_CODE" <> '810' and acc."CURRENCY_CODE" <> '643') as TURN_DEB_VAL,
sum(acc_tur.debet_amount_rub)as TURN_DEB_TOTAL ,
sum(acc_tur.CREDIT_AMOUNT_RUB) FILTER(where acc."CURRENCY_CODE" = '810' or acc."CURRENCY_CODE" = '643') as TURN_CRE_RUB ,
sum(acc_tur.CREDIT_AMOUNT_RUB) FILTER(where acc."CURRENCY_CODE" <> '810' and acc."CURRENCY_CODE" <> '643') as TURN_CRE_VAL ,
sum(acc_tur.CREDIT_AMOUNT_RUB) as TURN_CRE_TOTAL,
sum(acc_b.balance_out_rub) FILTER(where acc."CURRENCY_CODE" = '810' or acc."CURRENCY_CODE" = '643'  and acc_b.on_date = (ION_DATE  - interval '1 day')::date) as BALANCE_OUT_RUB ,
sum(acc_b.balance_out_rub) FILTER(where acc."CURRENCY_CODE" <> '810' and acc."CURRENCY_CODE" <> '643' and acc_b.on_date = (ION_DATE  - interval '1 day')::date) as BALANCE_OUT_VAL ,
sum(acc_b.balance_out_rub) FILTER(where  acc_b.on_date = (ION_DATE  - interval '1 day')::date) 
as BALANCE_OUT_TOTAL 
from DS.MD_LEDGER_ACCOUNT_S as ldg
join ds.md_account_d as acc on ldg."LEDGER_ACCOUNT" = left(acc."ACCOUNT_NUMBER",5)::integer
left join DM.DM_ACCOUNT_TURNOVER_F as acc_tur on acc_tur.account_rk = acc."ACCOUNT_RK" and acc_tur.on_date between (ION_DATE - interval '1 month')::date and
(ION_DATE  - interval '1 day')::date
left join dm.dm_account_balance_f as acc_b on acc_b.on_date = (ION_DATE  - interval '1 day')::date and acc_b.account_rk = acc."ACCOUNT_RK"
group by ldg."LEDGER_ACCOUNT";


INSERT INTO dm.dm_f101_round_f 
SELECT
	FROM_DATE,
	TO_DATE,
	"CHAPTER",
	table_1."LEDGER_ACCOUNT",
	CHARACTERISTIC,
	BALANCE_IN_RUB,
	BALANCE_IN_VAL,
	BALANCE_IN_TOTAL,
	TURN_DEB_RUB,
	TURN_DEB_VAL,
	TURN_DEB_TOTAL,
	TURN_CRE_RUB,
	TURN_CRE_VAL,
	TURN_CRE_TOTAL,
	BALANCE_out_RUB,
	BALANCE_out_VAL,
	BALANCE_out_TOTAL
FROM
	table_1
	JOIN table_2 ON table_1."LEDGER_ACCOUNT" = table_2."LEDGER_ACCOUNT";
	
drop table table_1;
drop table table_2;


		end ;
 $$ LANGUAGE PLPGSQL;