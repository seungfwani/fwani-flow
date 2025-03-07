def finish(data):
    output = f"finish function output: {data}"
    print(output)
    return output


def run(*args, **kwargs):
    return finish(kwargs.get("data"))
