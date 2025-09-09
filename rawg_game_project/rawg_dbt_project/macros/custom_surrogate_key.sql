{# Since there is a compatabiity issue with duckdb and dbt utils witht the surrogate key, I am creating my own macro to handle it. #}

{% macro custom_surrogate_key(columns) %}
    md5(concat_ws('||', {{ columns | map('string') | join(', ') }}))
{% endmacro %}