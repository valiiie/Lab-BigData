with source as (
    select * from {{ source('bronze', 'users') }}
),

orders as (
    select
        user_uuid,
        min(cast(date as timestamptz)) as first_order_date
    from {{ source('bronze', 'orders') }}
    group by user_uuid
),

typed as (
    select
        cast(s.uuid as uuid) as user_id,
        trim(s.username) as username,
        lower(trim(s.username)) as username_normalized,
        trim(s.name) as name,
        upper(trim(s.sex)) as sex,
        lower(trim(mail)) as email,
        cast(s.birthdate as date) as raw_birthdate,
        o.first_order_date,
        split_part(s.address, chr(10), 1) as street,
        split_part(s.address, chr(10), 2) as address_line_2
    from source s
    left join orders o on s.uuid = o.user_uuid
)

select
    user_id,
    username,
    username_normalized,
    name,
    sex,
    email,
    case
        when raw_birthdate > cast(first_order_date as date) then null
        else raw_birthdate
    end as birthdate,
    (raw_birthdate <= cast(first_order_date as date)) as birthdate_is_valid,
    street,
    nullif(regexp_extract(address_line_2, '^(.+), [A-Z]{2} \d{5}$', 1), '') as city,
    regexp_extract(address_line_2, '([A-Z]{2}) (\d{5})$', 1) as state,
    regexp_extract(address_line_2, '([A-Z]{2}) (\d{5})$', 2) as zip_code
from typed