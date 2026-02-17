{{ config(materialized='view') }}

{% set symbols = ['btc', 'eth', 'sol', 'bnb', 'ada', 'doge', 'xrp', 'dot'] %}

{% for symbol in symbols %}
    SELECT 
        '{{ symbol | upper }}' as symbol, 
        price, 
        TO_TIMESTAMP(event_time / 1000) as event_time
    FROM {{ 'prices_' ~ symbol }}
    {% if not loop.last %} UNION ALL {% endif %}
{% endfor %}