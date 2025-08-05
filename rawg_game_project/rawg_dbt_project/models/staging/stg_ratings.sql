with final as (
select
    raw.id as game_id,
    r.rating.id as rating_id,
    r.rating.title as rating_title,
    r.rating.count as rating_count,
    r.rating.percent as rating_percent
from
    {{ source('rawg', 'games_raw') }} raw,
    unnest(raw.ratings) as r(rating)
)

select
    *
from 
    final

