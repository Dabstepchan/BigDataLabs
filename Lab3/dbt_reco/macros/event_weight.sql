{# Вес действия при выборе категории интереса: покупка и корзина говорят об интересе сильнее просмотра #}
{% macro event_weight(event_type) -%}
    case {{ event_type }} when 'purchase' then 5 when 'cart' then 3 when 'view' then 1 else 0 end
{%- endmacro %}
