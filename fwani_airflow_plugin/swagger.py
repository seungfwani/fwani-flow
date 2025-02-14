from flask import Blueprint, jsonify
from flask_swagger import swagger

swagger_bp = Blueprint("swagger", __name__, url_prefix="/api/docs")


@swagger_bp.route("/swagger.json", methods=["GET"])
def swagger_docs():
    """
    Generates Swagger documentation for custom APIs.
    """
    from flask import current_app
    swag = swagger(current_app)
    swag['info'] = {
        "title": "Custom Plugin API",
        "version": "1.0"
    }
    return jsonify(swag)


@swagger_bp.route("/", methods=["GET"])
def swagger_ui():
    """
    Serve the Swagger UI.
    :return:
    """
    return """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Swagger UI</title>
        <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/swagger-ui/4.15.5/swagger-ui.css">
    </head>
    <body>
        <div id="swagger-ui"></div>
        <script src="https://cdnjs.cloudflare.com/ajax/libs/swagger-ui/4.15.5/swagger-ui-bundle.js"></script>
        <script src="https://cdnjs.cloudflare.com/ajax/libs/swagger-ui/4.15.5/swagger-ui-standalone-preset.js"></script>
        <script>
            const ui = SwaggerUIBundle({
                url: "/api/docs/swagger.json",
                dom_id: '#swagger-ui',
                presets: [
                    SwaggerUIBundle.presets.apis,
                    SwaggerUIStandalonePreset
                ],
                layout: "StandaloneLayout"
            });
        </script>
    </body>
    </html>
    """
