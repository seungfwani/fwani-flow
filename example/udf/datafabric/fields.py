from typing import List


def select_fields(data: dict, field: List[str]):
    import pandas as pd
    df = pd.DataFrame.from_dict(data, orient="tight")
    print(df)
    return df[field].to_dict(orient="tight")


if __name__ == '__main__':
    from example.udf.datafabric.fetch_model import get_data

    result = select_fields(get_data("mariadb-test.datafabric.datafabric.대전광역시_행정구역별_택시_승하차_통계_8월"),
                           ["동구 원동", "동구 효동", "동구 가오동"])
    print(result)
