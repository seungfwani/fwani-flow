"""
Code 템플릿 입니다.

*주의* 함수명(run) 변경하면 안됩니다.

run 함수의 body 부분을 채워주세요.
parameter 와 return 은 pandas.DataFrame 입니다.

parameter 값은 본 코드블럭에 연결된 직전 노드(메타타입 or 코드블록)의 output 입니다.
parameter 의 이름은 수정 가능합니다.

# ex) 아래의 경우 parameter 는 2개가 되어야합니다.
def run(param1, param2):
    ...
meta1 ---
        |
        |------> codeblock1
        |
meta2 ---
"""
import pandas as pd


def run({{ params }}):
    result = pd.DataFrame()
    return result
