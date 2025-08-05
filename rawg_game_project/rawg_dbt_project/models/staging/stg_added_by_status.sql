with final as (
select
    id as game_id,
    added_by_status['yet'] as yet,
    added_by_status['owned'] as owned,
    added_by_status['beaten'] as beaten,
    added_by_status['toplay'] as to_play,
    added_by_status['dropped'] as dropped,
    added_by_status['playing'] as playing
from
    {{ source('rawg', 'games_raw') }}
)

select
    *
from 
    final

