with final as (
select
    raw.id as game_id,
    t.tags.id as tag_id,
    t.tags.name as tag_name,
    t.tags.slug as tag_slug,
    t.tags.language as tag_language,
    t.tags.games_count as tag_games_count,
    t.tags.image_background as tag_image_background
from
    {{ source('rawg', 'games_raw') }} raw,
    unnest(raw.tags) as t(tags)
)

select
    *
from 
    final

