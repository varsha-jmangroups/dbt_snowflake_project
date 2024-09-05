with employee_data as (
    select 
        EMPLOYEE_ID,
        FIRST_NAME,
        LAST_NAME,
        EMAIL,
        {{ clean_phone_number('PHONE_NUMBER') }} AS PHONE_NO,  -- MACRO IS APPLIED
        HIRE_DATE,
        POSITION,
        SALARY,
        STORE_ID,
        EMPLOYMENT_STATUS

    from {{ source('rawsource', 'EMPLOYEES') }}
)

select * from employee_data
