def process1(data):
    output = f"process1 function output: {data}"
    print(output)
    return output


def run(*args, **kwargs):
    return process1(kwargs.get("data"))
