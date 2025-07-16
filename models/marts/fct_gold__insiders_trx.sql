{{ config(
    materialized = 'incremental'
) }}

with 

fct_insiders_trx as (

    select 
        t.filing as date_filing,
        k.cik as id_cik,
        t.symbol as code_symbol,
        t.reporting_name as desc_reporting_name,
        t.relationship as desc_relationship,
        t.trans_date as date_transaction,
        case when t.purchase_sale like '%PURCHASE%' then 1 else 0 end as is_purchase,
        t.shares as amt_shares,
        case when t.purchase_sale like '%PURCHASE%' then t.shares else -t.shares end as amt_signed_shares,
        t.price as price_share,
        t.amount as amt_transaction,
        case when t.purchase_sale like '%PURCHASE%' then t.amount else -t.amount end as amt_signed_transaction,
        cast(t.d_i = 'D' as int) as is_owner,
        current_timestamp() as created_at

    from {{ ref('stg_bronze__insiders_trx') }} t
    inner join {{ ref('stg_bronze__index_keys') }} k
    on t.symbol = k.ticker

    {% if is_incremental() %}
        where t.filing > (select max(date_filing) from {{ this }})
    {% endif %}

)

select 
        date_filing,
        id_cik,
        code_symbol,
        desc_reporting_name,
        desc_relationship,
        date_transaction,
        is_purchase,
        amt_shares,
        amt_signed_shares,
        sum(amt_signed_shares) over (
            partition by desc_reporting_name
            order by date_transaction, date_filing
            rows unbounded preceding
        ) as amt_shares_balance,
        price_share,
        amt_transaction,
        amt_signed_transaction,
        sum(amt_signed_transaction) over (
            partition by desc_reporting_name
            order by date_transaction, date_filing
            rows unbounded preceding
        ) as amt_transaction_balance,
        is_owner,
        created_at

from fct_insiders_trx