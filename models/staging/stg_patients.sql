with source as (
    select * from {{ source('raw', 'PATIENTS') }}
),

renamed as (
    select
        -- Primary Key
        patient_id,

        -- Demographics
        first                                           as first_name,
        middle                                          as middle_name,
        last                                            as last_name,
        prefix                                          as name_prefix,
        suffix                                          as name_suffix,
        maiden                                          as maiden_name,
        birthdate                                       as date_of_birth,
        deathdate                                       as date_of_death,
        gender,
        race,
        ethnicity,
        marital                                         as marital_status,

        -- Location
        address,
        city,
        state,
        county,
        zip                                             as zip_code,
        lat                                             as latitude,
        lon                                             as longitude,
        birthplace,

        -- Identifiers
        ssn,
        drivers                                         as drivers_license,
        passport,
        fips                                            as fips_code,

        -- Financial
        healthcare_expenses,
        healthcare_coverage,
        income,

        -- Metadata
        synthea_city,

        -- Derived
        case
            when deathdate is null then 'Active'
            else 'Deceased'
        end                                             as patient_status,

        datediff('year', birthdate, current_date())     as age_years,

        case
            when datediff('year', birthdate, current_date()) < 18  then '0-17'
            when datediff('year', birthdate, current_date()) < 35  then '18-34'
            when datediff('year', birthdate, current_date()) < 50  then '35-49'
            when datediff('year', birthdate, current_date()) < 65  then '50-64'
            else '65+'
        end                                             as age_group

    from source
)

select * from renamed