with 

source as (

    select * from {{ source('db_landing', 'src_raw_time') }}

),

stg_time as (

    select
        cast(date as date) as date,
        cast(year as integer) as year,
        cast(month as integer) as month,
        cast(upper(month_name) as string) as month_name,
        cast(day as integer) as day,
        cast(day_of_week as integer) as day_of_week,
        cast(upper(weekday_name) as string) as weekday_name,
        cast(upper(is_weekend) as string) as is_weekend,
        cast(week as integer) as week,
        cast(quarter as integer) as quarter,
        current_timestamp() as created_at

    from source

)

select * from stg_time
