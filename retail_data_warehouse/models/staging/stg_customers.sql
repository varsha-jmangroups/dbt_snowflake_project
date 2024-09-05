with customer_data as (
    select 
        CUSTOMER_ID,
        DATE_OF_BIRTH,
        EMAIL,
        FIRST_NAME,
        GENDER,
        LAST_NAME,
        LOYALTY_PROGRAM_STATUS,
        MEMBERSHIP_LEVEL,
        {{ clean_phone_number('PHONE_NUMBER') }} AS PHONE_NO, -- MACRO IS APPLIED
        PREFERRED_CONTACT_METHOD,
        REGISTRATION_DATE
    FROM {{ source('rawsource','CUSTOMERS')}}
)

select * from customer_data