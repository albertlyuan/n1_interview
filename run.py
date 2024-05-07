import sqlite3
import std_member_info_functions
import queries
def build_std_member_info(con):
    query1 = std_member_info_functions.normal_query('roster_1')
    query2 = std_member_info_functions.format_date_query('roster_2')
    query3 = std_member_info_functions.normal_query('roster_3')
    query4 = std_member_info_functions.abbr_to_state_query('roster_4')
    query5 = std_member_info_functions.normal_query('roster_5')

    # create table
    std_member_info_functions.create_std_member_info(con)

    for q in [query1,query2,query3,query4,query5]:
        std_member_info_functions.insert_std_member_info(q, con)

def build_std_member_info_no_dupe(con):
    query1 = std_member_info_functions.normal_query('roster_1')
    query2 = std_member_info_functions.format_date_query('roster_2')
    query3 = std_member_info_functions.normal_query('roster_3')
    query4 = std_member_info_functions.abbr_to_state_query('roster_4')
    query5 = std_member_info_functions.normal_query('roster_5')

    # create table
    std_member_info_functions.create_std_member_info(con)

    for q in [query1,query2,query3,query4,query5]:
        std_member_info_functions.insert_std_member_info_no_dupe_memberid(q, con)


if __name__ == "__main__":
    con = sqlite3.connect("interview.db")
    # assume that we don't want to eliminate dupes yet
    build_std_member_info(con)
    # build_std_member_info_no_dupe(con) #if we do run this

    queries.num_distinct_members_apr2022(con)
    queries.duplicate_members(con)

    # using no duplicate data for all 
    # assume everyone has a score (no null zip/score/non join)
    queries.members_by_payer(con)
    queries.food_access_score_lt2(con)
    queries.avg_social_isolation_score(con)
    # assume we want the member_ids 
    queries.highest_algorex_sdoh_composite_score(con)

    con.close()
