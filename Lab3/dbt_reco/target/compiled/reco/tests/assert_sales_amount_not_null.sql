-- Сумма продажи в рублях заполнена для каждой покупки
select * from "warehouse"."main"."fact_sales" where amount_rub is null