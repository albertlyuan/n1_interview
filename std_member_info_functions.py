def normal_query(tablename):
    query = f"""
    select distinct Person_Id, First_Name, Last_Name, Dob, Street_Address, City, State, Zip, payer 
    from {tablename}
    where eligibility_start_date < date('2022-04-01')  
        and eligibility_end_date >= date('2022-05-01')  
    """
    return query

def format_date_query(tablename):
    query = f"""
    select distinct Person_Id, First_Name, Last_Name, 
        date(substr(Dob, 7,4)||'-'||substr(Dob, 1,2)||'-'||substr(Dob, 4,2)) as date_of_birth, 
        Street_Address, City, State, Zip, payer 
    from {tablename}
    where date(substr(eligibility_start_date, 7,4)||'-'||substr(eligibility_start_date, 1,2)||'-'||substr(eligibility_start_date, 4,2)) < date('2022-04-01')  
        and date(substr(eligibility_end_date, 7,4)||'-'||substr(eligibility_end_date, 1,2)||'-'||substr(eligibility_end_date, 4,2)) >= date('2022-05-01')  
    """
    return query
def abbr_to_state_query(tablename):
    query = f"""
    select distinct Person_Id, First_Name, Last_Name, Dob, Street_Address, City, 
        CASE State
        WHEN 'AL' THEN 'Alabama' 
        WHEN 'AK' THEN 'Alaska' 
        WHEN 'AZ' THEN 'Arizona' 
        WHEN 'AR' THEN 'Arkansas' 
        WHEN 'CA' THEN 'California' 
        WHEN 'CO' THEN 'Colorado' 
        WHEN 'CT' THEN 'Connecticut' 
        WHEN 'DE' THEN 'Delaware' 
        WHEN 'DC' THEN 'District of Columbia' 
        WHEN 'FL' THEN 'Florida' 
        WHEN 'GA' THEN 'Georgia' 
        WHEN 'HI' THEN 'Hawaii' 
        WHEN 'ID' THEN 'Idaho' 
        WHEN 'IL' THEN 'Illinois' 
        WHEN 'IN' THEN 'Indiana' 
        WHEN 'IA' THEN 'Iowa' 
        WHEN 'KS' THEN 'Kansas' 
        WHEN 'KY' THEN 'Kentucky' 
        WHEN 'LA' THEN 'Louisiana' 
        WHEN 'ME' THEN 'Maine' 
        WHEN 'MD' THEN 'Maryland' 
        WHEN 'MA' THEN 'Massachusetts' 
        WHEN 'MI' THEN 'Michigan' 
        WHEN 'MN' THEN 'Minnesota' 
        WHEN 'MS' THEN 'Mississippi' 
        WHEN 'MO' THEN 'Missouri' 
        WHEN 'MT' THEN 'Montana' 
        WHEN 'NE' THEN 'Nebraska' 
        WHEN 'NV' THEN 'Nevada' 
        WHEN 'NH' THEN 'New Hampshire' 
        WHEN 'NJ' THEN 'New Jersey' 
        WHEN 'NM' THEN 'New Mexico' 
        WHEN 'NY' THEN 'New York' 
        WHEN 'NC' THEN 'North Carolina' 
        WHEN 'ND' THEN 'North Dakota' 
        WHEN 'OH' THEN 'Ohio' 
        WHEN 'OK' THEN 'Oklahoma' 
        WHEN 'OR' THEN 'Oregon' 
        WHEN 'PA' THEN 'Pennsylvania' 
        WHEN 'RI' THEN 'Rhode Island' 
        WHEN 'SC' THEN 'South Carolina' 
        WHEN 'SD' THEN 'South Dakota' 
        WHEN 'TN' THEN 'Tennessee' 
        WHEN 'TX' THEN 'Texas' 
        WHEN 'UT' THEN 'Utah' 
        WHEN 'VT' THEN 'Vermont' 
        WHEN 'VA' THEN 'Virginia' 
        WHEN 'WA' THEN 'Washington' 
        WHEN 'WV' THEN 'West Virginia' 
        WHEN 'WI' THEN 'Wisconsin' 
        WHEN 'WY' THEN 'Wyoming' 
        WHEN 'AB' THEN 'Alberta' 
        WHEN 'BC' THEN 'British Columbia' 
        WHEN 'MB' THEN 'Manitoba' 
        WHEN 'NB' THEN 'New Brunswick' 
        WHEN 'NL' THEN 'Newfoundland and Labrador' 
        WHEN 'NT' THEN 'Northwest Territories' 
        WHEN 'NS' THEN 'Nova Scotia' 
        WHEN 'NU' THEN 'Nunavut' 
        WHEN 'ON' THEN 'Ontario' 
        WHEN 'PE' THEN 'Prince Edward Island' 
        WHEN 'QC' THEN 'Quebec' 
        WHEN 'SK' THEN 'Saskatchewan' 
        WHEN 'YT' THEN 'Yukon Territory' 
        ELSE NULL
        END as State,
        Zip, payer

    from {tablename}
    where eligibility_start_date < date('2022-04-01')  
            and eligibility_end_date >= date('2022-05-01')
    """
    return query

def insert_std_member_info(query, con):
    insert_std_member_info_query = f"""
    insert into std_member_info (member_id, member_first_name, member_last_name, date_of_birth, main_address, city, state, zip_code, payer)
    {query}
    """

    con.cursor().execute(insert_std_member_info_query)
    con.commit()

def insert_std_member_info_no_dupe_memberid(query, con):
    insert_std_member_info_query = f"""
    insert into std_member_info (member_id, member_first_name, member_last_name, date_of_birth, main_address, city, state, zip_code, payer)
    {query}
    and Person_Id not in (select member_id from std_member_info)
    """

    con.cursor().execute(insert_std_member_info_query)
    con.commit()

def create_std_member_info(con):

    create_std_member_info_query = """
    create table std_member_info (member_id, member_first_name, member_last_name, date_of_birth, main_address, city, state, zip_code, payer);
    """
    con.cursor().execute("drop table std_member_info")
    con.cursor().execute(create_std_member_info_query)
    con.commit()
