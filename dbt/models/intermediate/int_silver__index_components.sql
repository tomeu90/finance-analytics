with all_indices as (

    select symbol as code_symbol, 'SP500' as desc_index from {{ ref('stg_bronze__sp_index') }}
    union all
    select ticker as code_symbol, 'NASDAQ100' as desc_index from {{ ref('stg_bronze__nasdaq_index') }}
    union all
    select symbol as code_symbol, 'DJIA' as desc_index from {{ ref('stg_bronze__djia_index') }}
    union all
    select symbol as code_symbol, 'RUSSELL1000' as desc_index from {{ ref('stg_bronze__russell_index') }}

),

int_index_components as (

    select
        code_symbol,
        max(case when desc_index = 'SP500' then 1 else 0 end) as is_sp_index,
        max(case when desc_index = 'DJIA' then 1 else 0 end) as is_djia_index,
        max(case when desc_index = 'NASDAQ100' then 1 else 0 end) as is_nasdaq_index,
        max(case when desc_index = 'RUSSELL1000' then 1 else 0 end) as is_russell_index
    from all_indices
    group by code_symbol

)

select * from int_index_components
