-- Сумма продажи в рублях заполнена для каждой покупки
select * from {{ ref('fact_sales') }} where amount_rub is null
