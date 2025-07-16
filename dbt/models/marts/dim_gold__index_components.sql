with dim_index_components as (

    select symbol as code_symbol, 'SP500' as desc_index from {{ ref('stg_bronze__sp_index') }}
    union all
    select ticker as code_symbol, 'NASDAQ100' as desc_index from {{ ref('stg_bronze__nasdaq_index') }}
    union all
    select symbol as code_symbol, 'DJIA' as desc_index from {{ ref('stg_bronze__djia_index') }}
    union all
    select symbol as code_symbol, 'RUSSELL1000' as desc_index from {{ ref('stg_bronze__russell_index') }}

)

select 
    ik.ticker as code_symbol,
    coalesce(desc_index, 'OFF-INDEX') as desc_index,
    current_timestamp() as created_at

from {{ ref('stg_bronze__index_keys') }} ik
left join dim_index_components ic
    on ik.ticker = ic.code_symbol
