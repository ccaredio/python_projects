with final as (
select
    id,
    name,
    released,
    rating,
    metacritic,
    playtime,
    background_image,
    rating_top
from 
    {{ source('rawg', 'games_raw') }}
)

select
    *
from
    final
    