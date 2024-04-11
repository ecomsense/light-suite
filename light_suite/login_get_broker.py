from omspy_brokers.dhanhq import Dhanhq


def login(**user):
    dq = Dhanhq(user["userid"], user["access_token"])
    if dq.authenticate():
        print(f"login successful for user {user['userid']}")
        return dq
    else:
        print(f"login failed for user {user['userid']}")
        return None
