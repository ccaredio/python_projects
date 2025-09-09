with final as (
select
    cast(id as string) as game_id,
    cast(added_by_status['yet'] as integer) as yet,
    cast(added_by_status['owned'] as integer) as owned,
    cast(added_by_status['beaten'] as integer) as beaten,
    cast(added_by_status['toplay'] as integer) as to_play,
    cast(added_by_status['dropped'] as integer) as dropped,
    cast(added_by_status['playing'] as integer) as playing
from
    {{ source('rawg', 'games_raw') }}
)

select
    *
from 
    final

