with final as (
select
    raw.id as game_id,
    platform.platform.id as platform_id,
    platform.platform.name as platform_name,
    platform.platform.slug as platform_slug
from
    {{ source('rawg', 'games_raw') }} raw,
    unnest(raw.platforms) as p(platform)
)

select
    *
from
    final