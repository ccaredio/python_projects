with final as (
select
    id as game_id,
    name as game_name,
    released,
    rating,
    metacritic as metacritic_rating,
    playtime,
    background_image,
    rating_top,
    ratings_count,
    reviews_text_count,
    cast(added as integer) as added,
    suggestions_count,
    updated as updated_at,
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
    

