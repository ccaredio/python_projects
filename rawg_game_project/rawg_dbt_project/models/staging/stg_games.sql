with final as (
select
    id,
    name,
    released,
    rating,
    metacritic,
    playtime,
    background_image,
    rating_top,
    ratings_count,
    reviews_text_count,
    added,
    metacritic,
    suggestions_count,
    updated,
    score,
    clip,
    user_game,
    reviews_count,
    community_rating
from 
    {{ source('rawg', 'games_raw') }}
)

select
    *
from
    final
    