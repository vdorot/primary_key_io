import datetime
from uuid6 import UUID, uuid7


def uuid7_time_boundary(dt, lower=True):
    timestamp_ms = int(dt.timestamp() * 1000)
    uuid_int = (timestamp_ms & 0xFFFFFFFFFFFF) << 80
    fill_byte = "00" if lower else "ff"
    uuid_int = uuid_int | int(fill_byte * 10, 16)
    # using the UUID constructor from uuid6 library, which automatically populates the version and variant fields
    return UUID(int=uuid_int, version=7)


if __name__ == "__main__":
    dt = datetime.datetime.strptime("2019-10-01 15:00:00", "%Y-%m-%d %H:%M:%S").replace(tzinfo=datetime.timezone.utc)

    date_uuid = str(uuid7_time_boundary(dt, lower=True))

    cur_uuid = str(uuid7())

    p_uuid = (date_uuid[:14] + cur_uuid[14:]).replace("-", "")

    print(date_uuid)
    print(p_uuid)


    dt = datetime.datetime.strptime("2022-04-03 10:30:00", "%Y-%m-%d %H:%M:%S").replace(tzinfo=datetime.timezone.utc)

    ####################################################################################################################
    # Using an upper boundary with <=:
    ####################################################################################################################
    boundary_uuid = uuid7_time_boundary(dt, lower=True)

    earlier_uuid = uuid7_time_boundary(dt - datetime.timedelta(milliseconds=1), lower=False)

    # compare with earlier uuid
    print("%s\n<\n%s\n== %s\n" % (earlier_uuid, boundary_uuid, earlier_uuid < boundary_uuid))
    # 017feef9-743f-7fff-bfff-ffffffffffff
    # <
    # 017feef9-7440-7000-8000-000000000000
    # == True

    later_uuid = UUID('018ea382-283f-7bfc-addb-a6c06bf382da')  # 2024-04-03

    # compare with later uuid
    print("%s\n<\n%s\n== %s\n" % (later_uuid, boundary_uuid, later_uuid < boundary_uuid))
    # 018ea382-283f-7bfc-addb-a6c06bf382da
    # <
    # 017feef9-7440-7000-8000-000000000000
    # == False

    ####################################################################################################################
    # Using an upper boundary with <=:
    ####################################################################################################################

    upper_boundary_uuid = uuid7_time_boundary(dt - datetime.timedelta(milliseconds=1), lower=False)

    print("%s\n<=\n%s\n== %s\n" % (earlier_uuid, upper_boundary_uuid, earlier_uuid <= upper_boundary_uuid))
    # 017feef9-743f-7fff-bfff-ffffffffffff
    # <=
    # 017feef9-743f-7fff-bfff-ffffffffffff
    # == True

    print("%s\n<=\n%s\n== %s\n" % (later_uuid, upper_boundary_uuid, later_uuid <= upper_boundary_uuid))
    # 018ea382-283f-7bfc-addb-a6c06bf382da
    # <=
    # 017feef9-743f-7fff-bfff-ffffffffffff
    # == False
