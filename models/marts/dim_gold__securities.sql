with 

dim_securities as (

    select  
        ik.cik as id_cik,
        ik.ticker as code_symbol,
        ik.name as desc_security,
        coalesce(ik.exchange, 'OFF-EXCHANGE') as desc_exchange,
        coalesce(ic.is_sp_index, 0) as is_sp_index,
        coalesce(ic.is_nasdaq_index, 0) as is_nasdaq_index,
        coalesce(ic.is_djia_index, 0) as is_djia_index,
        coalesce(ic.is_russell_index, 0) as is_russell_index,
        coalesce(ik.sic, 0) as code_sic,
        current_timestamp() as created_at

    from {{ ref('stg_bronze__index_keys') }} ik
    left join {{ ref('int_silver__index_components') }} ic
    on ik.ticker = ic.code_symbol

)

select * from dim_securities