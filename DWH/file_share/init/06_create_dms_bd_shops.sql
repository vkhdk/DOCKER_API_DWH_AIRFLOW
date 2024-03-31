-- change the database to dwh_shops
\c dwh_shops;
CREATE SCHEMA dm;
CREATE TABLE dm.dm_revizion (
    shop_id int8 NULL,
	product_name varchar(50) NULL,
	revizion_date date NULL,
	total_count int8 NULL,
    total_value int8 NULL,
    nearest_price int8 NULL
);

CREATE OR REPLACE FUNCTION dm.dm_revizion_f()
 RETURNS void
 LANGUAGE plpgsql
AS $function$
BEGIN
    TRUNCATE TABLE dm.dm_revizion;
    INSERT INTO dm.dm_revizion (shop_id, product_name, revizion_date, total_count, total_value, nearest_price)
    SELECT pd.shop_id, 
           p.product_name, 
           p.revizion_date, 
           SUM(p.count) AS total_count, 
           SUM(p.price_sale_out * p.count) AS total_value, 
           MAX(pd.price_sale_in) AS nearest_price
    FROM bd_shops.product_deliv pd
    INNER JOIN bd_shops.product p ON pd.product_id = p.id AND pd.shop_id = p.shop_id AND pd.delivery_day <= p.revizion_date
    GROUP BY pd.shop_id, p.product_name, p.revizion_date;
END;
$function$
;

CREATE TABLE dm.dm_month_profit (
    shop_id int8 NULL,
	product_name varchar(50) NULL,
	revizion_month date NULL,
	month_profit float4 NULL,
    info varchar(50) NULL
);

CREATE OR REPLACE FUNCTION dm.dm_month_profit_f()
 RETURNS void
 LANGUAGE plpgsql
AS $function$
BEGIN
    TRUNCATE TABLE dm.dm_month_profit;
    INSERT INTO dm.dm_month_profit (shop_id, product_name, revizion_month, month_profit, info)
    -- count next revision date and how many products was found in next revision
    WITH next_revizion_t AS (
            SELECT p.shop_id , 
                p.id , 
                p.product_name ,
                p.revizion_date ,
                p.count as current_count ,
                p.price_sale_out ,
                -- if the next revision is not found, fill nulls by '2099-01-01'
                COALESCE(LEAD(p.revizion_date) 
                            OVER (PARTITION BY p.shop_id , p.revizion_date , p.id 
                                ORDER BY  p.revizion_date ), '2099-01-01' ) AS next_revizion_date , 
                -- if the next revision is not found, leave it null
                COALESCE(LEAD(p.count) 
                            OVER (PARTITION BY p.shop_id , p.revizion_date , p.id 
                                ORDER BY  p.revizion_date ), null ) as next_revizion_count
            FROM bd_shops.product p) ,
            -- count the volume of delivered products after the current revision
        calc_deliv_t AS (
            -- if no delivery is found, fill the nulls by 0
            SELECT next_revizion_t.id ,
                next_revizion_t.product_name ,
                next_revizion_t.shop_id ,
                next_revizion_t.revizion_date ,
                next_revizion_t.current_count ,
                next_revizion_t.price_sale_out ,
                next_revizion_t.next_revizion_date ,
                next_revizion_t.next_revizion_count ,
                COALESCE(pd.count , 0) as after_deliv_count
            FROM next_revizion_t
            LEFT JOIN bd_shops.product_deliv pd on pd.shop_id = next_revizion_t.shop_id 
                AND pd.product_id = next_revizion_t.id 
                AND pd.delivery_day >= next_revizion_t.revizion_date 
                AND pd.delivery_day <= next_revizion_t.next_revizion_date) ,
        -- count total_profit for each revision
        calc_total_profit AS (
            SELECT * , 
                -- add month
                DATE_TRUNC('month', calc_deliv_t.revizion_date):: date AS revizion_month ,
                case
                    -- if the next revision is not found, fill -1 for later analysis 
                    WHEN calc_deliv_t.next_revizion_count IS NULL
                        THEN -1
                    -- if the next revision is found, calc total_profit for each revision
                    WHEN calc_deliv_t.next_revizion_count IS NOT NULL
                        THEN (calc_deliv_t.current_count + 
                            calc_deliv_t.after_deliv_count - 
                            calc_deliv_t.next_revizion_count) * calc_deliv_t.price_sale_out
                    END AS total_profit
            FROM calc_deliv_t)
    SELECT calc_total_profit.shop_id ,
        calc_total_profit.product_name , 
        calc_total_profit.revizion_month ,
        CASE 
            WHEN SUM(calc_total_profit.total_profit) < 0
                    THEN -1
        ELSE SUM(calc_total_profit.total_profit)
        END AS month_profit ,
        CASE 
            WHEN SUM(calc_total_profit.total_profit) < 0
                    THEN 'No information about next revision'
        ELSE 'Ok'
        END as info
    FROM calc_total_profit
    GROUP BY calc_total_profit.shop_id, calc_total_profit.revizion_month , calc_total_profit.product_name;
END;
$function$
;