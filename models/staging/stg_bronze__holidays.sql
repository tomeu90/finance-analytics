with 

source as (

    select * from {{ source('db_landing', 'src_raw_holidays') }}

),

stg_holidays as (

    select
        cast(date as date) as date,
        cast(upper(holiday) as string) as holiday,
        current_timestamp() as created_at

    from source

)

select * from stg_holidays
