from minio import Minio


def write_model(data: dict, model_name: str):
    import pandas as pd
    df = pd.DataFrame.from_dict(data, orient="tight")
    print(df)

    import io

    csv_bytes = io.BytesIO()
    df.to_csv(csv_bytes, index=False)
    csv_bytes.seek(0)  # 꼭 해줘야 함!

    # ✅ MinIO 클라이언트 설정
    client = Minio(
        "192.168.107.19:9000",
        access_key="root",
        secret_key="!BigDaTa99%",
        secure=False,
    )

    client.put_object(
        bucket_name="platform-test",
        object_name=f"workflow-model/{model_name}/result.csv",
        data=csv_bytes,
        length=len(csv_bytes.getvalue()),
        content_type="text/csv"
    )

    # openmeta noti
    return df.to_dict(orient="tight")


if __name__ == '__main__':
    from example.udf.datafabric.fetch_model import get_data

    result = write_model(get_data("mariadb-test.datafabric.datafabric.대전광역시_행정구역별_택시_승하차_통계_8월"),
                         model_name="udf_tester")
    print(result)
