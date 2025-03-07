def fetch(url):
    output = f"fetch data from: {url}"
    print(output)
    return output


def run(*args, **kwargs):
    return fetch(kwargs.get("url"))
