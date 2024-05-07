def num_distinct_members_apr2022(con):
    # How many distinct members are eligible in April 2022?
    query = """
    select count(distinct member_id)
    from std_member_info
    """
    print("distinct member_ids:", con.execute(query).fetchall())

def duplicate_members(con):
    # How many members were included more than once?
    query = """
    select count()
    from (select count(member_id) as cnt
        from std_member_info
        group by member_id)
    where cnt > 1
    """
    print("duplicate member_ids:", con.execute(query).fetchall())

def members_by_payer(con):
    # What is the breakdown of members by payer?
    query = """
    select payer, count(distinct member_id)
    from std_member_info
    group by payer
    """
    print("number members by payer:", con.execute(query).fetchall())

def food_access_score_lt2(con):
    # How many members live in a zip code with a food_access_score lower than 2?
    query = """
    select count(distinct member_id)
    from std_member_info
    left join model_scores_by_zip as modelscore on std_member_info.zip_code = modelscore.zcta
    where modelscore.food_access_score < 2
    """
    print("number members with food_access_score < 2:", con.execute(query).fetchall())

def avg_social_isolation_score(con):
    # What is the average social isolation score for the members?
    query = """
    select avg(social_isolation_score)
    from (select distinct member_id, zip_code from std_member_info) as std_member_info
    left join model_scores_by_zip as modelscore on std_member_info.zip_code = modelscore.zcta
    """
    print("average social_isolation_score:", con.execute(query).fetchall())

def highest_algorex_sdoh_composite_score(con):
    # Which members live in the zip code with the highest algorex_sdoh_composite_score?
    query = """
    with highest_score as (
        select algorex_sdoh_composite_score, zcta
        from model_scores_by_zip
        order by algorex_sdoh_composite_score desc
        limit 1
    )
    select count(*), zip_code, algorex_sdoh_composite_score
    from (select distinct member_id, zip_code from std_member_info) as std_member_info
    right join highest_score on std_member_info.zip_code = highest_score.zcta
    group by zip_code, algorex_sdoh_composite_score
    
    """
    print("members living in zip with highest algorex_sdoh_composite_score:", con.execute(query).fetchall())
