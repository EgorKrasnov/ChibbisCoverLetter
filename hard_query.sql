DECLARE @current_monday date = DATEADD(DAY,DATEDIFF(DAY,0,GETUTCDATE())/7*7,0);
DECLARE @second_monday date = DATEADD(DAY,DATEDIFF(DAY,0,GETUTCDATE())/7*7-14,0);
DECLARE @CURR_WEEK INT = DATEPART(week, GETUTCDATE());

USE v2_prod;
SET DATEFIRST 1;

--альт.поиск по адаптерам
with alt_table (iata_code
	   , [Исходный адаптер]
	   , [Альтернативный адаптер]
	   , log_count
	   , price_diff
	   , commision_diff
	   , incentive_diff
	   , segments_diff
	   , adapter_commission_diff
	   , payment_commission_diff
	   , sum_profit
	   , merged_name
	   , week_number
	   , [lagged price]) as (
                
        select iata_code
            , [Исходный адаптер]
            , [Альтернативный адаптер]
            , log_count
            , price_diff
            , commision_diff
            , incentive_diff
            , segments_diff
            , adapter_commission_diff
            , payment_commission_diff
            , sum_profit
            , merged_name
            , week_number
            , LAG(sum_profit,1,0) over (partition by merged_name order by week_number ) as 'lagged price'
        from (
            select ai.iata_code,
                ad1.name as 'Исходный адаптер',
                ad2.name as 'Альтернативный адаптер',
                count(rl.old_validating_carrier) as 'log_count',
                -- унаследованная бизнес логика
                sum(case 
                        when old_price_currency_rate_type=1 AND new_price_currency_rate_type=0 then rl.old_price/rl.old_price_currency_rate - rl.new_price*rl.new_price_currency_rate
                        when old_price_currency_rate_type=0 AND new_price_currency_rate_type=0  then rl.old_price*rl.old_price_currency_rate - rl.new_price*rl.new_price_currency_rate 
                        when old_price_currency_rate_type=0 AND new_price_currency_rate_type=1 then rl.old_price*rl.old_price_currency_rate - rl.new_price/rl.new_price_currency_rate 
                        else rl.old_price/rl.old_price_currency_rate - rl.new_price/rl.new_price_currency_rate end) as 'price_diff',
                sum(case 
                        when old_commission_currency_rate_type =1 AND new_commission_currency_rate_type =0  then rl.new_commission*rl.new_commission_currency_rate - rl.old_commission/rl.old_commission_currency_rate 
                        when old_commission_currency_rate_type =0 AND new_commission_currency_rate_type =0 then rl.new_commission*rl.new_commission_currency_rate - rl.old_commission*rl.old_commission_currency_rate  
                        when old_commission_currency_rate_type =0 AND  new_commission_currency_rate_type =1 then rl.new_commission/rl.new_commission_currency_rate - rl.old_commission*rl.old_commission_currency_rate 
                        else rl.new_commission/rl.new_commission_currency_rate - rl.old_commission/rl.old_commission_currency_rate end) as 'commision_diff',
                sum(case 
                        when old_incentive_currency_rate_type = 1 AND new_incentive_currency_rate_type = 0 then rl.new_incentive*rl.new_incentive_currency_rate - rl.old_incentive/rl.old_incentive_currency_rate 
                        when old_incentive_currency_rate_type = 0 AND new_incentive_currency_rate_type = 1 then  rl.new_incentive/rl.new_incentive_currency_rate - rl.old_incentive*rl.old_incentive_currency_rate 
                        when old_incentive_currency_rate_type = 0 AND new_incentive_currency_rate_type = 0 then  rl.new_incentive*rl.new_incentive_currency_rate - rl.old_incentive*rl.old_incentive_currency_rate 
                        else rl.new_incentive/rl.new_incentive_currency_rate - rl.old_incentive/rl.old_incentive_currency_rate end) as 'incentive_diff',
                sum(rl.new_segments - rl.old_segments) as 'segments_diff',
                sum(rl.old_adapter_commission - rl.new_adapter_commission) as 'adapter_commission_diff',
                sum(rl.old_payment_commission - rl.new_payment_commission) as 'payment_commission_diff',
                sum(rl.profit) as 'sum_profit'
                , iata_code+ad1.name+ad2.name as 'merged_name'
                , datepart (ISO_WEEK, dateadd (hour, 3, mr.reservation_date)) as 'week_number'
            from r_reissue_log rl (nolock)
            join c_adapter ad1 (nolock) on ad1.adapter_id = rl.old_adapter_id
            join c_adapter ad2 (nolock) on ad2.adapter_id = rl.adapter_id
            join m_reservation mr (nolock) on mr.reservation_id = rl.reservation_id
            join c_airline ai (nolock) on ai.airline_id = rl.old_validating_carrier
            left join (
                    select mr.reservation_id as 'id',
                        sum(case
                                when flight_reservation_flags & POWER(CAST(2 AS BIGINT),CAST(61 AS BIGINT)) = POWER(CAST(2 AS BIGINT),CAST(61 AS BIGINT))
                                then 1
                                else 0
                        end) as 'is_AFL_Litebag'
                    from r_flight_reservation rf (nolock)
                    left join r_product_reservation rp (nolock) on rp.product_reservation_id = rf.product_reservation_id
                    left join m_reservation mr (nolock) on rp.reservation_id = mr.reservation_id
                    group by mr.reservation_id
                        ) t1 on t1.id=mr.reservation_id 
            where DATEADD(hour, 3, rl.reissue_date) between @second_monday and @current_monday
            AND rl.reissue_type=4
            AND rl.is_applied=1
            AND mr.reservation_status_id in (7,13)
            AND t1.is_AFL_Litebag < 1 
            group by ai.iata_code, ad1.name, ad2.name, datepart (ISO_WEEK, dateadd (hour, 3, mr.reservation_date))
        ) as tq
        )

select iata_code as 'IATA code'
	   , [Исходный адаптер]
	   , [Альтернативный адаптер]
	   , log_count as 'Log count'
	   , price_diff as 'Price diff'
	   , commision_diff as 'Commission diff'
	   , incentive_diff as 'Incentive diff'
	   , segments_diff as 'Segments diff'
	   , adapter_commission_diff as 'Adapter comission diff'
	   , payment_commission_diff as 'Payment comission diff'
	   , sum_profit as 'Summ of profit'
       , 'abs vs -1' = 
			case 
				when [lagged price] = 0 then sum_profit
				else sum_profit-[lagged price]
			end
       , week_number
    from alt_table  
   where week_number = datepart(ISO_WEEK, @second_monday)+1
order by week_number desc, sum_profit desc