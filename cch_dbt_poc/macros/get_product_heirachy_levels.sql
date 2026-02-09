{% macro get_product_hierarchy_levels(column_name='PRDHA', table_alias='', align_col=80) -%}
    {%- set col_ref = table_alias ~ '.' ~ column_name if table_alias else column_name -%}
    {%- set specs = [
        {'suffix': 1, 'right': 13, 'left': 1},
        {'suffix': 2, 'right': 12, 'left': 1},
        {'suffix': 3, 'right': 11, 'left': 1},
        {'suffix': 4, 'right': 10, 'left': 1},
        {'suffix': 5, 'right': 9,  'left': 3},
        {'suffix': 6, 'right': 6,  'left': 2},
        {'suffix': 7, 'right': 4,  'left': 2},
        {'suffix': 8, 'right': 2,  'left': 2}
    ] -%}
    {%- for h in specs %}
    ,LEFT(
        RIGHT(
            RPAD(
                 CAST({{ col_ref }} AS VARCHAR)
                ,13
                ,'0'
            )
            ,{{ h.right }}
        )
        ,{{ h.left }}
    ){{ ' ' * (align_col - 6) }}AS "{{ 'material_product_hierarchy__' ~ h.suffix }}"
    {%- endfor %}
{%- endmacro %}