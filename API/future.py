""" Next steps  part of code which can be used to instead of query input
and accept var like user and date directly


def policy_count(user, underwriter=None, date=None):

    if underwriter is None and date is None:
        print("count policy per user")
    elif underwriter is not None and date is None:
        print("count policy per user per underwriter")
    elif date is not None and underwriter is None:
        print("count policy per user per month")
    else:
        print("do some")


# schema_policy = """
#  SELECT *
#    FROM public.policy
#    WHERE policy_id NOT IN
#        (SELECT policy_id
#         FROM public.finance
#         WHERE reason='cancellation_full_refund')
# """


# newuser_date = f"""
# select count(user_id) from
# (SELECT user_id,
#  MIN(policy_start_date: : date) AS user_lifecycle_startDate
#    FROM {schema_policy}
#    GROUP BY user_id) as f
#    where user_lifecycle_startDate = '{date}' """


"""

