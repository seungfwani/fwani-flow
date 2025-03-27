from typing import List


def select_fields(data: dict, conditions: List[(str, str, str)]):
    import pandas as pd
    df = pd.DataFrame.from_dict(data, orient="tight")
    print(df)
    compare_operator = {
        "<": lambda x, y: x < y,
        "<=": lambda x, y: x <= y,
        ">": lambda x, y: x > y,
        ">=": lambda x, y: x >= y,
        "==": lambda x, y: x == y,
        "!=": lambda x, y: x != y,
        "in": lambda x, y: x in y,
        "not in": lambda x, y: x not in y,
    }
    for condition in conditions:
        df = df[compare_operator[condition[1]](df[condition[0]], condition[2])]
    return df.to_dict(orient="tight")


if __name__ == '__main__':
    from example.udf.datafabric.fetch_model import get_data

    result = select_fields(get_data("mariadb-test.datafabric.datafabric.대전광역시_행정구역별_택시_승하차_통계_8월"),
                           [("동구 원동", ">", 200), ("동구 효동", ">", 180)])
    print(result)
