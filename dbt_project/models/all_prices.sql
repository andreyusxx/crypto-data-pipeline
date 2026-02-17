{{ config(materialized='view') }}

{% set symbols = ['btc', 'eth', 'sol', 'bnb', 'ada', 'doge', 'xrp', 'dot'] %}

{% for symbol in symbols %}
    SELECT DISTINCT
        '{{ symbol | upper }}' as symbol, 
        MAX(price) as price, 
        TO_TIMESTAMP(event_time / 1000) as event_time
    FROM {{ 'prices_' ~ symbol }}
    GROUP BY 1, 3
    {% if not loop.last %} UNION ALL {% endif %}
{% endfor %}