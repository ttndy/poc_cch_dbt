{% macro star_join(left_col, right_col) %}
    (
        {{ left_col }} = {{ right_col }}
        OR {{ right_col }} = '*'
    )
{% endmacro %}
