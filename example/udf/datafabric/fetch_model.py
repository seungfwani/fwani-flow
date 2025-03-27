import json

import requests


def get_data(model_name: str):
    model_data = requests.post(
        "http://192.168.109.254:30733/dolphin/v1/query/execute",
        data=json.dumps({
            "query": f"select * from `{model_name}`",
            "limit": 10,
        })
    )
    data = model_data.json()

    import pandas as pd
    df = pd.DataFrame(data.get("resultData").get("rows"), columns=data.get("resultData").get("columns"), )
    print(df)
    return df.to_dict(orient="tight")


if __name__ == '__main__':
    result = get_data("mariadb-test.datafabric.datafabric.대전광역시_행정구역별_택시_승하차_통계_8월")
    print(result)
