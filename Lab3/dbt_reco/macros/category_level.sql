{# Уровень иерархии category_code (electronics.smartphone -> electronics) #}
{% macro category_level(column, level) -%}
    split_part({{ column }}, '.', {{ level }})
{%- endmacro %}
