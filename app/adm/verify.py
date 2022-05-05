import time
import hmac
from hashlib import sha256
from config import SLACK_SIGNING_SECRET


def verify(request):
    try:
        timestamp = int(request.headers["X-Slack-Request-Timestamp"])
        slack_signature = request.headers["X-Slack-Signature"]
        body = request.get_data().decode()
        basestring = f"v0:{timestamp}:{body}".encode("utf-8")
    except:
        return False

    my_signature = (
            "v0=" + hmac.new(SLACK_SIGNING_SECRET, basestring, sha256).hexdigest()
    )
    if time.time() - int(timestamp) > 60:
        return False

    return my_signature == slack_signature