with final as (
select
    raw.id as game_id,
    genre.id as genre_id,
    genre.name as genre_name,
    genre.slug as genre_slug
from
    {{ source('rawg', 'games_raw') }} raw,
    unnest(raw.genres) as g(genre)
)

select
    *
from 
    final

