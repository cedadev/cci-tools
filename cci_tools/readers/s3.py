from cci_tools.core.utils import obsClient


def locate_content(bucket, prefix=None, mark=None):

    mark = None
    paging = True
    resp_size = 1000
    fileset = []
    while paging:
        resp = obsClient.listObjects(
            bucket, prefix=prefix, marker=mark, max_keys=resp_size
        )
        if resp.status >= 300:
            print("error", resp.status)
            break

        for content in resp.body.contents:
            fileset.append(content.key)

        if resp.body.is_truncated:
            mark = resp.body.next_marker
        else:
            paging = False

    return fileset
