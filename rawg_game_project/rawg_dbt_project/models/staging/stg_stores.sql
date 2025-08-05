with final as (
select
    raw.id as game_id,
    store.store.id as store_id,
    store.store.name as store_name,
    store.store.slug as store_slug
from
    {{ source('rawg', 'games_raw') }} raw,
    unnest(raw.stores) as s(store)
)

select
    *
from 
    final

