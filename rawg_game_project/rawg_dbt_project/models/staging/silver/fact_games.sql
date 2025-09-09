{{
  config(
    materialized='incremental',
    unique_key='pri_id',
    on_schema_change='fail'
  )
}}

with joined as (
    select
        games.game_id,
        platforms.platform_id,
        genres.genre_id,
        stores.store_id,
        ratings.rating_id,
        games.updated_at,
        games.released,
        games.rating,
        games.ratings_count,
        games.reviews_text_count,
        games.added,
        games.suggestions_count,
        games.score,
        games.reviews_count,
        added.yet as not_yet_added,
        added.owned,
        added.beaten,
        added.to_play,
        added.dropped,
        added.playing
    from    
        {{ ref('stg_games') }} as games
    join
        {{ ref('stg_platforms') }} as platforms on games.game_id = platforms.game_id
    join
        {{ ref('stg_genres') }} as genres on games.game_id = genres.game_id
    join
        {{ ref('stg_stores') }} as stores on games.game_id = stores.game_id
    join
        {{ ref('stg_ratings') }} as ratings on games.game_id = ratings.game_id
    join
        {{ ref('stg_added_by_status') }} as added on games.game_id = added.game_id

    {% if is_incremental() %}
        where games.updated_at >= coalesce((select max(updated_at) from {{ this }}), '1900-01-01')
    {% endif %}
)

select
    game_id,
    platform_id,
    genre_id,
    store_id,
    rating_id,
    updated_at,
    released,
    rating,
    ratings_count,
    reviews_text_count,
    added,
    suggestions_count,
    score,
    reviews_count,
    not_yet_added,
    owned,
    beaten,
    to_play,
    dropped,
    playing,
    {{ custom_surrogate_key([
        'game_id',
        'platform_id',
        'genre_id',
        'store_id',
        'rating_id'
    ]) }} as pri_id
from 
    joined
