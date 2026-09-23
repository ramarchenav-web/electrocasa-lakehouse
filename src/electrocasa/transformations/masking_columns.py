# DNI masking
spark.sql("""
CREATE OR REPLACE FUNCTION electrocasa.silver.mask_dni(dni STRING)
RETURNS STRING
RETURN CASE
    WHEN is_account_group_member('auditoria') THEN dni
    ELSE concat('***-***-', right(dni, 3))
END
""")

spark.sql("""
ALTER TABLE electrocasa.silver.empleados_silver
ALTER COLUMN dni
SET MASK electrocasa.silver.mask_dni
""")

# Salario masking
spark.sql("""
CREATE OR REPLACE FUNCTION electrocasa.silver.mask_salario(salario DECIMAL(12,2))
RETURNS DECIMAL(12,2)
RETURN CASE
    WHEN is_account_group_member('auditoria') THEN salario
    ELSE round(salario, -3)
END
""")

spark.sql("""
ALTER TABLE electrocasa.silver.empleados_silver
ALTER COLUMN salario
SET MASK electrocasa.silver.mask_salario
""")

display(
    spark.sql("""
    SELECT
        id_empleado,
        nombre,
        salario,
        dni
    FROM electrocasa.silver.empleados_silver
    ORDER BY id_empleado
    """)
)