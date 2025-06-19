import azure.functions as func
import logging
import json

app = func.FunctionApp()

@app.route(route="HttpExample", auth_level=func.AuthLevel.ANONYMOUS)
def HttpExample(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Request received for echo")

    try:
        req_body = req.get_json()
    except ValueError:
        return func.HttpResponse("Invalid JSON", status_code=400)

    return func.HttpResponse(
        json.dumps({"echo": req_body}),
        status_code=200,
        mimetype="application/json"
    )
