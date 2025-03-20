def process2(data):
    output = f"process2 function output: {data}"
    print(output)
    return output


def run(*args, **kwargs):
    return process2(kwargs.get("data"))
