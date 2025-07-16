with dim_time as (

    select
        t.date,
        t.year as date_year,
        t.month as date_month,
        t.month_name as desc_month,
        t.day as date_day,
        t.day_of_week as date_weekday,
        t.weekday_name as desc_weekday,
        cast(t.is_weekend = 'TRUE' as int) as is_weekend,
        cast(h.holiday is not null as int) as is_holiday,
        coalesce(h.holiday, '') as desc_holiday,
        t.week as date_week,
        t.quarter as date_quarter,
        current_timestamp() as created_at

    from {{ ref('stg_bronze__time') }} t
    left join {{ ref('stg_bronze__holidays') }} h
        on t.date = h.date

)

select
    date,
    date_year,
    date_month,
    desc_month,
    date_day,
    date_weekday,
    desc_weekday,
    is_weekend,
    is_holiday,
    desc_holiday,
    case
        when is_holiday = 1 then 0
        when is_weekend = 1 then 0
        else 1
    end as is_market_open,
    date_week,
    date_quarter,
    created_at

from dim_time
